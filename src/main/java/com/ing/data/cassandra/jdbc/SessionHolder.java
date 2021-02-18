/*
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.ing.data.cassandra.jdbc;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import com.google.common.cache.LoadingCache;
import com.ing.data.cassandra.jdbc.codec.BigintToBigDecimalCodec;
import com.ing.data.cassandra.jdbc.codec.DecimalToDoubleCodec;
import com.ing.data.cassandra.jdbc.codec.FloatToDoubleCodec;
import com.ing.data.cassandra.jdbc.codec.IntToLongCodec;
import com.ing.data.cassandra.jdbc.codec.LongToIntCodec;
import com.ing.data.cassandra.jdbc.codec.TimestampToLongCodec;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.ing.data.cassandra.jdbc.Utils.JSSE_KEYSTORE_PASSWORD_PROPERTY;
import static com.ing.data.cassandra.jdbc.Utils.JSSE_KEYSTORE_PROPERTY;
import static com.ing.data.cassandra.jdbc.Utils.JSSE_TRUSTSTORE_PASSWORD_PROPERTY;
import static com.ing.data.cassandra.jdbc.Utils.JSSE_TRUSTSTORE_PROPERTY;
import static com.ing.data.cassandra.jdbc.Utils.SSL_CONFIG_FAILED;

/**
 * Holds a {@link Session} shared among multiple {@link CassandraConnection} objects.
 * <p>
 *     This class uses reference counting to track if active {@code CassandraConnections} still use the session. When
 *     the last {@link CassandraConnection} has been closed, the {@code Session} is closed.
 * </p>
 */
@SuppressWarnings("UnstableApiUsage")
class SessionHolder {
    static final String URL_KEY = "jdbcUrl";

    private static final Logger log = LoggerFactory.getLogger(SessionHolder.class);

    final Session session;
    final Properties properties;
    private final LoadingCache<Map<String, String>, SessionHolder> parentCache;
    private final Map<String, String> cacheKey;
    private final AtomicInteger references = new AtomicInteger();

    /**
     * Constructs a {@code SessionHolder} instance.
     *
     * @param params        The parameters.
     * @param parentCache   The cache.
     * @throws SQLException when something went wrong during the {@link Session} creation.
     */
    SessionHolder(final Map<String, String> params, final LoadingCache<Map<String, String>, SessionHolder> parentCache)
        throws SQLException {
        this.cacheKey = params;
        this.parentCache = parentCache;

        final String url = params.get(URL_KEY);

        // Parse the URL into a set of Properties and replace double quote marks (") by simple quotes (') to handle the
        // fact that double quotes (") are not valid characters in URIs.
        this.properties = Utils.parseURL(url.replace("\"", "'"));

        // Other properties in parameters come from the initial call to connect(), they take priority.
        params.keySet().stream()
            .filter(key -> !URL_KEY.equals(key))
            .forEach(key -> this.properties.put(key, params.get(key)));
        if (log.isDebugEnabled()) {
            log.debug("Final Properties to Connection: {}", this.properties);
        }

        this.session = createSession(this.properties);
    }

    /**
     * Indicates that a {@link CassandraConnection} has been closed and stopped using this object.
     */
    void release() {
        int newRef;
        while (true) {
            final int ref = this.references.get();
            // We set to -1 after the last release, to distinguish it from the initial state.
            if (ref == 1) {
                newRef = -1;
            } else {
                newRef = ref - 1;
            }
            if (this.references.compareAndSet(ref, newRef)) {
                break;
            }
        }
        if (newRef == -1) {
            log.debug("Released last reference to {}, closing Session.", this.cacheKey.get(URL_KEY));
            dispose();
        } else {
            log.debug("Released reference to {}, new count = {}.", this.cacheKey.get(URL_KEY), newRef);
        }
    }

    /**
     * Method called when a {@link CassandraConnection} tries to acquire a reference to this object.
     *
     * @return {@code true} if the reference was acquired successfully, {@code false} otherwise.
     */
    boolean acquire() {
        while (true) {
            final int ref = this.references.get();
            if (ref < 0) {
                // We raced with the release of the last reference, the caller will need to create a new session.
                log.debug("Failed to acquire reference to {}.", this.cacheKey.get(URL_KEY));
                return false;
            }
            if (this.references.compareAndSet(ref, ref + 1)) {
                log.debug("Acquired reference to {}, new count = {}.", this.cacheKey.get(URL_KEY), ref + 1);
                return true;
            }
        }
    }

    @SuppressWarnings("resource")
    private Session createSession(final Properties properties) throws SQLException {
        final String hosts = properties.getProperty(Utils.TAG_SERVER_NAME);
        final int port = Integer.parseInt(properties.getProperty(Utils.TAG_PORT_NUMBER));
        final String keyspace = properties.getProperty(Utils.TAG_DATABASE_NAME);
        final String username = properties.getProperty(Utils.TAG_USER, StringUtils.EMPTY);
        final String password = properties.getProperty(Utils.TAG_PASSWORD, StringUtils.EMPTY);
        final String loadBalancingPolicy = properties.getProperty(Utils.TAG_LOAD_BALANCING_POLICY, StringUtils.EMPTY);
        final String localDatacenter = properties.getProperty(Utils.TAG_LOCAL_DATACENTER, StringUtils.EMPTY);
        final String retryPolicy = properties.getProperty(Utils.TAG_RETRY_POLICY, StringUtils.EMPTY);
        final String reconnectPolicy = properties.getProperty(Utils.TAG_RECONNECT_POLICY, StringUtils.EMPTY);
        final boolean debugMode = Boolean.TRUE.toString().equals(properties.getProperty(Utils.TAG_DEBUG,
            StringUtils.EMPTY));
        final String enableSslValue = properties.getProperty(Utils.TAG_ENABLE_SSL);
        final String sslEngineFactoryClassName = properties.getProperty(Utils.TAG_SSL_ENGINE_FACTORY,
            StringUtils.EMPTY);
        final boolean sslEnabled = Boolean.TRUE.toString().equals(enableSslValue)
            || (enableSslValue == null && StringUtils.isNotEmpty(sslEngineFactoryClassName));

        // Instantiate the session builder and set the contact points.
        final CqlSessionBuilder builder = CqlSession.builder();
        final ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder =
            DriverConfigLoader.programmaticBuilder();
        driverConfigLoaderBuilder.withBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE, true);
        builder.addContactPoints(Arrays.stream(hosts.split("--"))
            .map(host -> InetSocketAddress.createUnresolved(host, port))
            .collect(Collectors.toList())
        );

        // Set credentials when applicable.
        if (username.length() > 0) {
            builder.withAuthCredentials(username, password);
        }

        // The DefaultLoadBalancingPolicy requires to specify a local data center.
        builder.withLocalDatacenter(localDatacenter);
        if (loadBalancingPolicy.length() > 0) {
            // if a custom load balancing policy has been given in the JDBC URL, parse it and add it to the cluster
            // builder.
            try {
                driverConfigLoaderBuilder.withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                    loadBalancingPolicy);
            } catch (final Exception e) {
                if (debugMode) {
                    throw new SQLNonTransientConnectionException(e);
                }
                log.warn("Error occurred while parsing load balancing policy: " + e.getMessage()
                    + " / Forcing to DefaultLoadBalancingPolicy...");
                driverConfigLoaderBuilder.withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                    DefaultLoadBalancingPolicy.class.getSimpleName());
            }
        }

        if (retryPolicy.length() > 0) {
            // if retry policy has been given in the JDBC URL, parse it and add it to the cluster builder.
            try {
                driverConfigLoaderBuilder.withString(DefaultDriverOption.RETRY_POLICY_CLASS, retryPolicy);
            } catch (final Exception e) {
                if (debugMode) {
                    throw new SQLNonTransientConnectionException(e);
                }
                log.warn("Error occurred while parsing retry policy: " + e.getMessage() + " / skipping...");
            }
        }

        if (reconnectPolicy.length() > 0) {
            // if reconnection policy has been given in the JDBC URL, parse it and add it to the cluster builder.
            try {
                final Map<DriverOption, Object> parsedPolicy = Optional.ofNullable(
                    Utils.parseReconnectionPolicy(reconnectPolicy)).orElse(new HashMap<>());

                driverConfigLoaderBuilder.withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS,
                    (String) parsedPolicy.get(DefaultDriverOption.RECONNECTION_POLICY_CLASS));
                if (parsedPolicy.containsKey(DefaultDriverOption.RECONNECTION_BASE_DELAY)) {
                    driverConfigLoaderBuilder.withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY,
                        (Duration) parsedPolicy.get(DefaultDriverOption.RECONNECTION_BASE_DELAY));
                }
                if (parsedPolicy.containsKey(DefaultDriverOption.RECONNECTION_MAX_DELAY)) {
                    driverConfigLoaderBuilder.withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY,
                        (Duration) parsedPolicy.get(DefaultDriverOption.RECONNECTION_MAX_DELAY));
                }
            } catch (final Exception e) {
                if (debugMode) {
                    throw new SQLNonTransientConnectionException(e);
                }
                log.warn("Error occurred while parsing reconnection policy: " + e.getMessage() + " / skipping...");
            }
        }

        // Declare and register codecs.
        final List<TypeCodec<?>> codecs = new ArrayList<>();
        codecs.add(new TimestampToLongCodec());
        codecs.add(new LongToIntCodec());
        codecs.add(new IntToLongCodec());
        codecs.add(new BigintToBigDecimalCodec());
        codecs.add(new DecimalToDoubleCodec());
        codecs.add(new FloatToDoubleCodec());
        builder.addTypeCodecs(codecs.toArray(new TypeCodec[]{}));

        builder.withKeyspace(keyspace);
        builder.withConfigLoader(driverConfigLoaderBuilder.build());

        // SSL configuration.
        if (sslEnabled) {
            if (StringUtils.isNotEmpty(sslEngineFactoryClassName)) {
                configureSslEngineFactory(builder, sslEngineFactoryClassName);
            } else {
                configureDefaultSslEngineFactory(builder, driverConfigLoaderBuilder);
            }
        }

        try {
            return builder.build();
        } catch (final DriverException e) {
            if (this.session != null) {
                this.session.close();
            }
            throw new SQLNonTransientConnectionException(e);
        }
    }

    void configureSslEngineFactory(final CqlSessionBuilder builder, final String sslEngineFactoryClassName)
        throws SQLNonTransientConnectionException {
        try {
            final Class<?> sslEngineFactoryClass = Class.forName(sslEngineFactoryClassName);
            final SslEngineFactory sslEngineFactory =
                (SslEngineFactory) sslEngineFactoryClass.getConstructor().newInstance();
            builder.withSslEngineFactory(sslEngineFactory);
        } catch (final Exception e) {
            throw new SQLNonTransientConnectionException(String.format(SSL_CONFIG_FAILED, e.getMessage()), e);
        }
    }

    void configureDefaultSslEngineFactory(final CqlSessionBuilder builder,
                                          final ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder) {
        // Get JSSE properties and add them to the driver context in order to use DefaultSslEngineFactory.
        driverConfigLoaderBuilder.withString(DefaultDriverOption.SSL_TRUSTSTORE_PATH,
            System.getProperty(JSSE_TRUSTSTORE_PROPERTY));
        driverConfigLoaderBuilder.withString(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD,
            System.getProperty(JSSE_TRUSTSTORE_PASSWORD_PROPERTY));
        driverConfigLoaderBuilder.withString(DefaultDriverOption.SSL_KEYSTORE_PATH,
            System.getProperty(JSSE_KEYSTORE_PROPERTY));
        driverConfigLoaderBuilder.withString(DefaultDriverOption.SSL_KEYSTORE_PASSWORD,
            System.getProperty(JSSE_KEYSTORE_PASSWORD_PROPERTY));
        final DriverContext driverContext = new DefaultDriverContext(driverConfigLoaderBuilder.build(),
            ProgrammaticArguments.builder().build());
        builder.withSslEngineFactory(new DefaultSslEngineFactory(driverContext));
    }

    private void dispose() {
        // No one else has a reference to the parent cluster, and only one Session was created from it, so close the
        // session and invalidate the cache.
        this.session.close();
        this.parentCache.invalidate(this.cacheKey);
    }
}
