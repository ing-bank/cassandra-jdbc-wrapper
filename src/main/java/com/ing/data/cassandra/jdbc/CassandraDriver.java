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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.ing.data.cassandra.jdbc.utils.DriverUtil.getDriverProperty;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.safeParseVersion;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.CONNECTION_CREATION_FAILED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.PROTOCOL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONTACT_POINTS;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_PASSWORD;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_USER;

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

    // Caching sessions so that multiple CassandraConnections created with the same parameters use the same Session.
    private final LoadingCache<Map<String, String>, SessionHolder> sessionsCache = Caffeine.newBuilder()
        .build(new CacheLoader<Map<String, String>, SessionHolder>() {
            @Override
            public SessionHolder load(final Map<String, String> params) throws Exception {
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
        Properties properties = props;
        if (props == null) {
            properties = new Properties();
        }
        final DriverPropertyInfo[] info = new DriverPropertyInfo[2];

        info[0] = new DriverPropertyInfo(TAG_USER, properties.getProperty(TAG_USER));
        info[0].description = "The 'user' property";

        info[1] = new DriverPropertyInfo(TAG_PASSWORD, properties.getProperty(TAG_PASSWORD));
        info[1].description = "The 'password' property";

        return info;
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
