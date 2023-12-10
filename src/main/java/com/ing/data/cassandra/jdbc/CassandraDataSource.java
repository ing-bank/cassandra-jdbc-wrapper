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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.ing.data.cassandra.jdbc.utils.ContactPoint;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NO_INTERFACE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.PROTOCOL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONSISTENCY_LEVEL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONTACT_POINTS;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_DATABASE_NAME;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_LOCAL_DATACENTER;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_PASSWORD;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_USER;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.createSubName;

/**
 * Cassandra data source: implementation class for {@link DataSource} and {@link ConnectionPoolDataSource}.
 */
public class CassandraDataSource implements ConnectionPoolDataSource, DataSource {

    // Check the driver.
    static {
        try {
            Class.forName("com.ing.data.cassandra.jdbc.CassandraDriver");
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The Cassandra data source description.
     */
    protected static final String DATA_SOURCE_DESCRIPTION = "Cassandra Data Source";
    /**
     * The contact points of the data source.
     */
    protected List<ContactPoint> contactPoints;
    /**
     * The database name. In case of Cassandra, i.e. the keyspace used as data source.
     */
    protected String databaseName;
    /**
     * The username used to connect to the data source.
     */
    protected String user;
    /**
     * The password used to connect to the data source.
     */
    protected String password;
    /**
     * The consistency level.
     * <p>
     *     See <a href="https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/dml/dmlConfigConsistency.html">
     *     consistency level documentation</a> for further details.
     * </p>
     */
    protected String consistency = null;
    /**
     * The local datacenter.
     */
    protected String localDataCenter = null;

    /**
     * Constructor.
     *
     * @param contactPoints The contact points.
     * @param keyspace      The keyspace.
     * @param user          The username used to connect.
     * @param password      The password used to connect.
     * @param consistency   The consistency level.
     */
    public CassandraDataSource(final List<ContactPoint> contactPoints, final String keyspace, final String user,
                               final String password, final String consistency) {
        this(contactPoints, keyspace, user, password, consistency, null);
    }

    /**
     * Constructor specifying a local datacenter (required to use {@link DefaultLoadBalancingPolicy}).
     *
     * @param contactPoints     The contact points.
     * @param keyspace          The keyspace.
     * @param user              The username used to connect.
     * @param password          The password used to connect.
     * @param consistency       The consistency level.
     * @param localDataCenter   The local datacenter.
     */
    public CassandraDataSource(final List<ContactPoint> contactPoints, final String keyspace, final String user,
                               final String password, final String consistency, final String localDataCenter) {
        if (contactPoints != null && !contactPoints.isEmpty()) {
            setContactPoints(contactPoints);
        }
        if (consistency != null) {
            setConsistency(consistency);
        }
        if (localDataCenter != null) {
            setLocalDataCenter(localDataCenter);
        }
        setDatabaseName(keyspace);
        setUser(user);
        setPassword(password);
    }

    /**
     * Gets the data source description.
     *
     * @return The data source description.
     */
    public String getDescription() {
        return DATA_SOURCE_DESCRIPTION;
    }

    /**
     * Gets the contact points of the data source.
     *
     * @return The contact points of the data source.
     */
    public List<ContactPoint> getContactPoints() {
        return this.contactPoints;
    }

    /**
     * Sets the contact points of the data source.
     *
     * @param contactPoints The contact points of the data source.
     */
    public void setContactPoints(final List<ContactPoint> contactPoints) {
        this.contactPoints = contactPoints;
    }

    /**
     * Gets the consistency level.
     * <p>
     *     See <a href="https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html">
     *     consistency level documentation</a> for further details.
     * </p>
     *
     * @return The consistency level.
     */
    public String getConsistency() {
        return this.consistency;
    }

    /**
     * Sets the consistency level.
     * <p>
     *     See <a href="https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html">
     *     consistency level documentation</a> and {@link ConsistencyLevel} to get the acceptable values.
     * </p>
     *
     * @param consistency The consistency level.
     */
    public void setConsistency(final String consistency) {
        this.consistency = consistency;
    }

    /**
     * Gets the database name. In case of Cassandra, i.e. the keyspace used as data source.
     *
     * @return The database name. In case of Cassandra, i.e. the keyspace used as data source.
     */
    public String getDatabaseName() {
        return this.databaseName;
    }

    /**
     * Sets the database name. In case of Cassandra, i.e. the keyspace used as data source.
     *
     * @param databaseName The database name. In case of Cassandra, i.e. the keyspace used as data source.
     */
    public void setDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Gets the username used to connect to the data source.
     *
     * @return The username used to connect to the data source.
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Sets the username used to connect to the data source.
     *
     * @param user The username used to connect to the data source.
     */
    public void setUser(final String user) {
        this.user = user;
    }

    /**
     * Gets the password used to connect to the data source.
     *
     * @return The password used to connect to the data source.
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * Sets the password used to connect to the data source.
     *
     * @param password The password used to connect to the data source.
     */
    public void setPassword(final String password) {
        this.password = password;
    }

    /**
     * Gets the local datacenter. It is required with the {@link DefaultLoadBalancingPolicy}.
     *
     * @return The local datacenter.
     */
    public String getLocalDataCenter() {
        return this.localDataCenter;
    }

    /**
     * Sets the local datacenter. It is required with the {@link DefaultLoadBalancingPolicy}.
     *
     * @param localDataCenter The local datacenter.
     */
    public void setLocalDataCenter(final String localDataCenter) {
        this.localDataCenter = localDataCenter;
    }

    @Override
    public CassandraConnection getConnection() throws SQLException {
        return getConnection(null, null);
    }

    @Override
    public CassandraConnection getConnection(final String user, final String password) throws SQLException {
        final Properties props = new Properties();
        this.user = user;
        this.password = password;

        if (this.contactPoints != null && !this.contactPoints.isEmpty()) {
            props.put(TAG_CONTACT_POINTS, this.contactPoints);
        }
        if (this.databaseName != null) {
            props.setProperty(TAG_DATABASE_NAME, this.databaseName);
        }
        if (user != null) {
            props.setProperty(TAG_USER, user);
        }
        if (password != null) {
            props.setProperty(TAG_PASSWORD, password);
        }
        if (this.consistency != null) {
            props.setProperty(TAG_CONSISTENCY_LEVEL, consistency);
        }
        if (this.localDataCenter != null) {
            props.setProperty(TAG_LOCAL_DATACENTER, localDataCenter);
        }

        final String url = PROTOCOL.concat(createSubName(props));
        return (CassandraConnection) DriverManager.getConnection(url, props);
    }

    @Override
    public int getLoginTimeout() {
        return DriverManager.getLoginTimeout();
    }

    @Override
    public PrintWriter getLogWriter() {
        return DriverManager.getLogWriter();
    }

    @Override
    public void setLoginTimeout(final int timeout) {
        DriverManager.setLoginTimeout(timeout);
    }

    @Override
    public void setLogWriter(final PrintWriter writer) {
        DriverManager.setLogWriter(writer);
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(this.getClass());
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        } else {
            throw new SQLException(String.format(NO_INTERFACE, iface.getSimpleName()));
        }
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public PooledCassandraConnection getPooledConnection() throws SQLException {
        return new PooledCassandraConnection(getConnection());
    }

    @Override
    public PooledCassandraConnection getPooledConnection(final String user, final String password) throws SQLException {
        return new PooledCassandraConnection(getConnection(user, password));
    }
}
