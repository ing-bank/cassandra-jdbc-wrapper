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
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
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

/**
 * Holds a {@link Session} shared among multiple {@link CassandraConnection} objects.
 * <p>
 * This class uses reference counting to track if active CassandraConnections still use
 * the Session. When the last CassandraConnection has closed, the Session gets closed.
 */
class SessionHolder {
    private static final Logger log = LoggerFactory.getLogger(SessionHolder.class);
    static final String URL_KEY = "jdbcUrl";

    private final LoadingCache<Map<String, String>, SessionHolder> parentCache;
    private final Map<String, String> cacheKey;

    private final AtomicInteger references = new AtomicInteger();
    final Session session;
    final Properties properties;

    SessionHolder(final Map<String, String> params, final LoadingCache<Map<String, String>, SessionHolder> parentCache)
        throws SQLException {
        this.cacheKey = params;
        this.parentCache = parentCache;

        final String url = params.get(URL_KEY);

        // parse the URL into a set of Properties
        // replace " by ' to handle the fact that " is not a valid character in URIs
        properties = Utils.parseURL(url.replace("\"", "'"));

        // other properties in params come from the initial call to connect(), they take priority
        for (final String key : params.keySet()) {
            if (!URL_KEY.equals(key)) {
                properties.put(key, params.get(key));
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Final Properties to Connection: {}", properties);
        }

        session = createSession(properties);
    }

    /**
     * Indicates that a CassandraConnection has closed and stopped using this object.
     */
    void release() {
        int newRef;
        while (true) {
            final int ref = references.get();
            // We set to -1 after the last release, to distinguish it from the initial state
            newRef = (ref == 1) ? -1 : ref - 1;
            if (references.compareAndSet(ref, newRef)) {
                break;
            }
        }
        if (newRef == -1) {
            log.debug("Released last reference to {}, closing Session", cacheKey.get(URL_KEY));
            dispose();
        } else {
            log.debug("Released reference to {}, new count = {}", cacheKey.get(URL_KEY), newRef);
        }
    }

    /**
     * Called when a CassandraConnection tries to acquire a reference to this object.
     *
     * @return whether the reference was acquired successfully
     */
    boolean acquire() {
        while (true) {
            final int ref = references.get();
            if (ref < 0) {
                // We raced with the release of the last reference, the caller will need to create a new session
                log.debug("Failed to acquire reference to {}", cacheKey.get(URL_KEY));
                return false;
            }
            if (references.compareAndSet(ref, ref + 1)) {
                log.debug("Acquired reference to {}, new count = {}", cacheKey.get(URL_KEY), ref + 1);
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
        final String loadBalancingPolicy = properties.getProperty(Utils.TAG_LOADBALANCING_POLICY, StringUtils.EMPTY);
        final String localDatacenter = properties.getProperty(Utils.TAG_LOCAL_DATACENTER, StringUtils.EMPTY);
        final String retryPolicy = properties.getProperty(Utils.TAG_RETRY_POLICY, StringUtils.EMPTY);
        final String reconnectPolicy = properties.getProperty(Utils.TAG_RECONNECT_POLICY, StringUtils.EMPTY);
        final boolean debugMode = Boolean.TRUE.toString().equals(properties.getProperty(Utils.TAG_DEBUG,
            StringUtils.EMPTY));

        final CqlSessionBuilder builder = CqlSession.builder();
        final ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder =
            DriverConfigLoader.programmaticBuilder();
        builder.addContactPoints(Arrays.stream(hosts.split("--"))
            .map(host -> InetSocketAddress.createUnresolved(host, port))
            .collect(Collectors.toList())
        );

        // Set credentials when applicable
        if (username.length() > 0) {
            builder.withAuthCredentials(username, password);
        }

        driverConfigLoaderBuilder.withBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE, true);

        // The DefaultLoadBalancingPolicy requires to specify a local data center.
        builder.withLocalDatacenter(localDatacenter);
        if (loadBalancingPolicy.length() > 0) {
            // if a custom load balancing policy has been given in the JDBC URL, parse it and add it to the cluster
            // builder
            try {
                driverConfigLoaderBuilder.withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                    loadBalancingPolicy);
            } catch (final Exception e) {
                if (debugMode) {
                    throw new SQLNonTransientConnectionException(e);
                }
                log.warn("Error occurred while parsing load balancing policy :" + e.getMessage()
                    + " / Forcing to DefaultLoadBalancingPolicy...");
                driverConfigLoaderBuilder.withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                    DefaultLoadBalancingPolicy.class.getSimpleName());
            }
        }

        if (retryPolicy.length() > 0) {
            // if retry policy has been given in the JDBC URL, parse it and add it to the cluster builder
            try {
                driverConfigLoaderBuilder.withString(DefaultDriverOption.RETRY_POLICY_CLASS, retryPolicy);
            } catch (final Exception e) {
                if (debugMode) {
                    throw new SQLNonTransientConnectionException(e);
                }
                log.warn("Error occurred while parsing retry policy :" + e.getMessage() + " / skipping...");
            }
        }

        if (reconnectPolicy.length() > 0) {
            // if reconnection policy has been given in the JDBC URL, parse it and add it to the cluster builder
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
                log.warn("Error occurred while parsing reconnection policy :" + e.getMessage() + " / skipping...");
            }
        }

        // Declare and register codecs
        final List<TypeCodec<?>> codecs = new ArrayList<>();
        codecs.add(new TimestampToLongCodec());
        codecs.add(new LongToIntCodec());
        codecs.add(new IntToLongCodec());
        codecs.add(new BigintToBigDecimalCodec());
        codecs.add(new DecimalToDoubleCodec());
        codecs.add(new FloatToDoubleCodec());
        builder.addTypeCodecs(codecs.toArray(new TypeCodec[]{}));
        // end of codec register

        builder.withKeyspace(keyspace);
        builder.withConfigLoader(driverConfigLoaderBuilder.build());

        try {
            return builder.build();
        } catch (final DriverException e) {
            if (session != null) {
                session.close();
            }
            throw new SQLNonTransientConnectionException(e);
        }
    }

    private void dispose() {
        // No one else has a reference to the parent Cluster, and only one Session was created from it:
        session.close();
        parentCache.invalidate(cacheKey);
    }
}
