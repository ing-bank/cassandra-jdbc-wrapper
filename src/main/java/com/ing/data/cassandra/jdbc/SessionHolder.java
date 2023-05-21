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
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.ing.data.cassandra.jdbc.codec.BigintToBigDecimalCodec;
import com.ing.data.cassandra.jdbc.codec.DecimalToDoubleCodec;
import com.ing.data.cassandra.jdbc.codec.FloatToDoubleCodec;
import com.ing.data.cassandra.jdbc.codec.IntToLongCodec;
import com.ing.data.cassandra.jdbc.codec.LongToIntCodec;
import com.ing.data.cassandra.jdbc.codec.SmallintToIntCodec;
import com.ing.data.cassandra.jdbc.codec.TimestampToLongCodec;
import com.ing.data.cassandra.jdbc.codec.TinyintToIntCodec;
import com.ing.data.cassandra.jdbc.codec.VarintToIntCodec;
import com.ing.data.cassandra.jdbc.utils.Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.ing.data.cassandra.jdbc.utils.Utils.JSSE_KEYSTORE_PASSWORD_PROPERTY;
import static com.ing.data.cassandra.jdbc.utils.Utils.JSSE_KEYSTORE_PROPERTY;
import static com.ing.data.cassandra.jdbc.utils.Utils.JSSE_TRUSTSTORE_PASSWORD_PROPERTY;
import static com.ing.data.cassandra.jdbc.utils.Utils.JSSE_TRUSTSTORE_PROPERTY;
import static com.ing.data.cassandra.jdbc.utils.Utils.SSL_CONFIG_FAILED;

/**
 * Holds a {@link Session} shared among multiple {@link CassandraConnection} objects.
 * <p>
 *     This class uses reference counting to track if active {@code CassandraConnections} still use the session. When
 *     the last {@link CassandraConnection} has been closed, the {@code Session} is closed.
 * </p>
 */
class SessionHolder {
    static final String URL_KEY = "jdbcUrl";

    private static final Logger LOG = LoggerFactory.getLogger(SessionHolder.class);

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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Final Properties to Connection: {}", this.properties);
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
            LOG.debug("Released last reference to {}, closing Session.", this.cacheKey.get(URL_KEY));
            dispose();
        } else {
            LOG.debug("Released reference to {}, new count = {}.", this.cacheKey.get(URL_KEY), newRef);
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
                LOG.debug("Failed to acquire reference to {}.", this.cacheKey.get(URL_KEY));
                return false;
            }
            if (this.references.compareAndSet(ref, ref + 1)) {
                LOG.debug("Acquired reference to {}, new count = {}.", this.cacheKey.get(URL_KEY), ref + 1);
                return true;
            }
        }
    }

    private Session createSession(final Properties properties) throws SQLException {
        File configurationFile = null;
        final String configurationFilePath = properties.getProperty(Utils.TAG_CONFIG_FILE, StringUtils.EMPTY);
        if (StringUtils.isNotBlank(configurationFilePath)) {
            configurationFile = new File(configurationFilePath);
            if (configurationFile.exists()) {
                // We remove some parameters to use the values defined into the specified configuration file
                // instead.
                this.properties.remove(Utils.TAG_CONSISTENCY_LEVEL);
                this.properties.remove(Utils.TAG_LOCAL_DATACENTER);
                this.properties.remove(Utils.TAG_USER);
                this.properties.remove(Utils.TAG_PASSWORD);
                this.properties.remove(Utils.TAG_ENABLE_SSL);
                this.properties.remove(Utils.TAG_SSL_ENGINE_FACTORY);
                this.properties.remove(Utils.TAG_SSL_HOSTNAME_VERIFICATION);
                this.properties.remove(Utils.TAG_REQUEST_TIMEOUT);
                this.properties.remove(Utils.TAG_CONNECT_TIMEOUT);
                this.properties.remove(Utils.TAG_KEEP_ALIVE);
                this.properties.remove(Utils.TAG_TCP_NO_DELAY);
                LOG.info("The configuration file {} will be used and will override the parameters defined into the "
                    + "JDBC URL except contact points and keyspace.", configurationFilePath);
            } else {
                LOG.warn("The configuration file {} cannot be found, it will be ignored.", configurationFilePath);
            }
        }

        final String hosts = properties.getProperty(Utils.TAG_SERVER_NAME);
        final int port = Integer.parseInt(properties.getProperty(Utils.TAG_PORT_NUMBER));
        final String cloudSecureConnectBundle = properties.getProperty(Utils.TAG_CLOUD_SECURE_CONNECT_BUNDLE);
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
        final String enableSslHostnameVerification = properties.getProperty(Utils.TAG_SSL_HOSTNAME_VERIFICATION);
        final boolean sslHostnameVerificationEnabled = Boolean.TRUE.toString().equals(enableSslHostnameVerification)
            || enableSslHostnameVerification == null;
        final String requestTimeoutRawValue = properties.getProperty(Utils.TAG_REQUEST_TIMEOUT);
        Integer requestTimeout = null;
        if (NumberUtils.isParsable(requestTimeoutRawValue)) {
            requestTimeout = Integer.parseInt(requestTimeoutRawValue);
        }

        // Instantiate the session builder and set the contact points.
        final CqlSessionBuilder builder = CqlSession.builder();
        final ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder =
            DriverConfigLoader.programmaticBuilder();
        driverConfigLoaderBuilder.withBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE, true);
        if (StringUtils.isNotBlank(cloudSecureConnectBundle)) {
            driverConfigLoaderBuilder.withString(DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE,
                cloudSecureConnectBundle);
            LOG.info("Cloud secure connect bundle used. Host(s) {} will be ignored.", hosts);
        } else {
            builder.addContactPoints(Arrays.stream(hosts.split("--"))
	            .map(host -> InetSocketAddress.createUnresolved(host, port))
	            .collect(Collectors.toList())
            );
        }

        // Set request timeout (in milliseconds) if defined.
        if (requestTimeout != null && requestTimeout > 0) {
            driverConfigLoaderBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
                Duration.of(requestTimeout, ChronoUnit.MILLIS));
        }

        // Set socket options if defined.
        configureSocketOptions(driverConfigLoaderBuilder, properties);

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
                LOG.warn("Error occurred while parsing load balancing policy: {} / Forcing to "
                    + "DefaultLoadBalancingPolicy...", e.getMessage());
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
                LOG.warn("Error occurred while parsing retry policy: {} / skipping...", e.getMessage());
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
                LOG.warn("Error occurred while parsing reconnection policy: {} / skipping...", e.getMessage());
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
        codecs.add(new VarintToIntCodec());
        codecs.add(new SmallintToIntCodec());
        codecs.add(new TinyintToIntCodec());
        builder.addTypeCodecs(codecs.toArray(new TypeCodec[]{}));

        builder.withKeyspace(keyspace);
        builder.withConfigLoader(driverConfigLoaderBuilder.build());

        // SSL configuration.
        if (StringUtils.isBlank(cloudSecureConnectBundle)) {
            if (sslEnabled) {
                if (StringUtils.isNotEmpty(sslEngineFactoryClassName)) {
                    configureSslEngineFactory(builder, sslEngineFactoryClassName);
                } else {
                    driverConfigLoaderBuilder.withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION,
                        sslHostnameVerificationEnabled);
                    configureDefaultSslEngineFactory(builder, driverConfigLoaderBuilder);
                }
            }
        } else {
            LOG.info("Cloud secure connect bundle used. SSL will always be enabled. All manual SSL "
                + "configuration(s) will be ignored.");
        }

        // Set the configuration from a configuration file if defined.
        if (configurationFile != null) {
            builder.withConfigLoader(DriverConfigLoader.fromFile(configurationFile));
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

    void configureSocketOptions(final ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder,
                                final Properties properties) {
        // Parse options received from JDBC URL
        final String connectTimeoutRawValue = properties.getProperty(Utils.TAG_CONNECT_TIMEOUT);
        Integer connectTimeout = null;
        if (NumberUtils.isParsable(connectTimeoutRawValue)) {
            connectTimeout = Integer.parseInt(connectTimeoutRawValue);
        }
        final String enableTcpNoDelay = properties.getProperty(Utils.TAG_TCP_NO_DELAY);
        final boolean tcpNoDelayEnabled = Boolean.TRUE.toString().equals(enableTcpNoDelay) || enableTcpNoDelay == null;
        final String enableTcpKeepAlive = properties.getProperty(Utils.TAG_KEEP_ALIVE);
        final boolean tcpKeepAliveEnabled = Boolean.TRUE.toString().equals(enableTcpKeepAlive);

        // Apply configuration
        if (connectTimeout != null) {
            driverConfigLoaderBuilder.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT,
                Duration.of(connectTimeout, ChronoUnit.MILLIS));
        }
        driverConfigLoaderBuilder.withBoolean(DefaultDriverOption.SOCKET_TCP_NODELAY, tcpNoDelayEnabled);
        driverConfigLoaderBuilder.withBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE, tcpKeepAliveEnabled);
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
