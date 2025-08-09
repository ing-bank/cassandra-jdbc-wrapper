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

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.ing.data.cassandra.jdbc.utils.DriverUtil.buildPropertyInfo;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.getDriverProperty;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.safeParseVersion;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.CONNECTION_CREATION_FAILED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.PROTOCOL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_ACTIVE_PROFILE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_ASTRA_REGION;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_AWS_REGION;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_AWS_SECRET_NAME;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_AWS_SECRET_REGION;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CLOUD_SECURE_CONNECT_BUNDLE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_COMPLIANCE_MODE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONFIG_FILE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONNECT_TIMEOUT;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONSISTENCY_LEVEL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONTACT_POINTS;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CUSTOM_CODECS;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_DEBUG;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_ENABLE_SSL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_FETCH_SIZE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_KEEP_ALIVE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_LOAD_BALANCING_POLICY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_LOCAL_DATACENTER;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_PASSWORD;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_RECONNECT_POLICY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_REQUEST_TIMEOUT;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_RETRY_POLICY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_SERIAL_CONSISTENCY_LEVEL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_SSL_ENGINE_FACTORY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_SSL_HOSTNAME_VERIFICATION;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_TCP_NO_DELAY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_TOKEN;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_USER;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_USE_KERBEROS;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_USE_SIG_V4;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.parseURL;
import static com.ing.data.cassandra.jdbc.utils.WarningConstants.PROPERTIES_PARSING_FROM_URL_FAILED;

/**
 * The Cassandra driver implementation.
 */
public class CassandraDriver implements Driver {

    static {
        // Register the CassandraDriver with DriverManager.
        try {
            final CassandraDriver driverInstance = new CassandraDriver();
            DriverManager.registerDriver(driverInstance);
        } catch (final SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(CassandraDriver.class);

    // Caching sessions so that multiple CassandraConnections created with the same parameters use the same Session.
    private final LoadingCache<Map<String, String>, SessionHolder> sessionsCache = Caffeine.newBuilder()
        .build(new CacheLoader<Map<String, String>, SessionHolder>() {
            @Override
            public SessionHolder load(@Nonnull final Map<String, String> params) throws Exception {
                return new SessionHolder(params, sessionsCache);
            }
        });

    @Override
    public boolean acceptsURL(final String url) {
        return url.startsWith(PROTOCOL);
    }

    @Override
    public Connection connect(final String url, final Properties properties) throws SQLException {
        if (acceptsURL(url)) {
            final Map<String, String> params = new HashMap<>();
            final Enumeration<Object> keys = properties.keys();
            while (keys.hasMoreElements()) {
                final String key = (String) keys.nextElement();
                if (!TAG_CONTACT_POINTS.equals(key)) {
                    params.put(key, properties.getProperty(key));
                }
            }
            params.put(SessionHolder.URL_KEY, url);

            final Map<String, String> cacheKey = Collections.unmodifiableMap(params);

            try {
                while (true) {
                    // Get (or create) the corresponding Session from the cache.
                    final SessionHolder sessionHolder = this.sessionsCache.get(cacheKey);

                    if (sessionHolder != null && sessionHolder.acquire()) {
                        return new CassandraConnection(sessionHolder);
                    }
                    // If we failed to acquire a connection, it means we raced with the release of the last reference
                    // to the session (which also removes it from the cache, see SessionHolder class for details).
                    // Loop to try again, that will cause the cache to create a new instance.
                }
            } catch (final Exception e) {
                final Throwable cause = e.getCause();
                if (cause instanceof SQLException) {
                    throw (SQLException) cause;
                }
                throw new SQLNonTransientConnectionException(CONNECTION_CREATION_FAILED, e);
            }
        }
        // Signal it is the wrong driver for this <protocol:sub_protocol>.
        return null;
    }

    @Override
    public int getMajorVersion() {
        return safeParseVersion(getDriverProperty("driver.version")).getMajor();
    }

    @Override
    public int getMinorVersion() {
        return safeParseVersion(getDriverProperty("driver.version")).getMinor();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties props) {
        Properties properties;
        try {
            properties = parseURL(url);
            for (Map.Entry<Object, Object> propEntry : props.entrySet()) {
                properties.putIfAbsent(propEntry.getKey(), propEntry.getValue());
            }
        } catch (final SQLException e) {
            LOG.warn(PROPERTIES_PARSING_FROM_URL_FAILED, e);
            properties = new Properties(props);
        }

        // Define the list of availableProperties.
        final List<String> availableProperties = Arrays.asList(TAG_USER, TAG_PASSWORD, TAG_LOCAL_DATACENTER, TAG_DEBUG,
            TAG_CONSISTENCY_LEVEL, TAG_SERIAL_CONSISTENCY_LEVEL, TAG_ACTIVE_PROFILE, TAG_FETCH_SIZE,
            TAG_LOAD_BALANCING_POLICY, TAG_RETRY_POLICY, TAG_RECONNECT_POLICY, TAG_ENABLE_SSL, TAG_SSL_ENGINE_FACTORY,
            TAG_SSL_HOSTNAME_VERIFICATION, TAG_CLOUD_SECURE_CONNECT_BUNDLE, TAG_USE_KERBEROS, TAG_REQUEST_TIMEOUT,
            TAG_CONNECT_TIMEOUT, TAG_TCP_NO_DELAY, TAG_KEEP_ALIVE, TAG_CONFIG_FILE, TAG_COMPLIANCE_MODE, TAG_AWS_REGION,
            TAG_AWS_SECRET_NAME, TAG_AWS_SECRET_REGION, TAG_USE_SIG_V4, TAG_TOKEN, TAG_ASTRA_REGION, TAG_CUSTOM_CODECS);

        final List<DriverPropertyInfo> info = new ArrayList<>();
        for (String propertyName : availableProperties) {
            info.add(buildPropertyInfo(propertyName, properties.get(propertyName)));
        }

        return info.toArray(new DriverPropertyInfo[]{});
    }

    /**
     * Reports whether this driver is a genuine JDBC Compliant™ driver. A driver may only report {@code true} here if
     * it passes the JDBC compliance tests; otherwise it is required to return {@code false}.
     * <p>
     *     For Cassandra, this is not possible as it is not SQL92 compliant (among others).
     * </p>
     */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }
}
