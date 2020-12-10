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

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Properties;
import java.util.logging.Logger;

import static com.ing.data.cassandra.jdbc.Utils.HOST_REQUIRED;
import static com.ing.data.cassandra.jdbc.Utils.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.Utils.NO_INTERFACE;
import static com.ing.data.cassandra.jdbc.Utils.PROTOCOL;
import static com.ing.data.cassandra.jdbc.Utils.TAG_CONSISTENCY_LEVEL;
import static com.ing.data.cassandra.jdbc.Utils.TAG_CQL_VERSION;
import static com.ing.data.cassandra.jdbc.Utils.TAG_DATABASE_NAME;
import static com.ing.data.cassandra.jdbc.Utils.TAG_LOCAL_DATACENTER;
import static com.ing.data.cassandra.jdbc.Utils.TAG_PASSWORD;
import static com.ing.data.cassandra.jdbc.Utils.TAG_PORT_NUMBER;
import static com.ing.data.cassandra.jdbc.Utils.TAG_SERVER_NAME;
import static com.ing.data.cassandra.jdbc.Utils.TAG_USER;
import static com.ing.data.cassandra.jdbc.Utils.createSubName;

public class CassandraDataSource implements ConnectionPoolDataSource, DataSource {

    static {
        try {
            Class.forName("com.ing.data.cassandra.jdbc.CassandraDriver");
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected static final String description = "Cassandra Data Source";

    protected String serverName;

    protected int portNumber = 9042;

    protected String databaseName;

    protected String user;

    protected String password;

    protected String version = null;

    protected String consistency = null;

    protected String localDataCenter = null;

    public CassandraDataSource(final String host, final int port, final String keyspace, final String user,
                               final String password, final String version, final String consistency) {
        this(host, port, keyspace, user, password, version, consistency, null);
    }

    public CassandraDataSource(final String host, final int port, final String keyspace, final String user,
                               final String password, final String version, final String consistency,
                               final String localDataCenter) {
        if (host != null) {
            setServerName(host);
        }
        if (port != -1) {
            setPortNumber(port);
        }
        if (version != null) {
            setVersion(version);
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

    public String getDescription() {
        return description;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(final String serverName) {
        this.serverName = serverName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(final String version) {
        this.version = version;
    }

    public String getConsistency() {
        return consistency;
    }

    public void setConsistency(final String consistency) {
        this.consistency = consistency;
    }

    public int getPortNumber() {
        return portNumber;
    }

    public void setPortNumber(final int portNumber) {
        this.portNumber = portNumber;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
    }

    public String getUser() {
        return user;
    }

    public void setUser(final String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(final String password) {
        this.password = password;
    }

    public String getLocalDataCenter() {
        return localDataCenter;
    }

    public void setLocalDataCenter(final String localDataCenter) {
        this.localDataCenter = localDataCenter;
    }

    public CassandraConnection getConnection() throws SQLException {
        return getConnection(null, null);
    }

    public CassandraConnection getConnection(final String user, final String password) throws SQLException {
        final Properties props = new Properties();

        this.user = user;
        this.password = password;

        if (this.serverName != null) props.setProperty(TAG_SERVER_NAME, this.serverName);
        else throw new SQLNonTransientConnectionException(HOST_REQUIRED);
        props.setProperty(TAG_PORT_NUMBER, "" + this.portNumber);
        if (this.databaseName != null) props.setProperty(TAG_DATABASE_NAME, this.databaseName);
        if (user != null) props.setProperty(TAG_USER, user);
        if (password != null) props.setProperty(TAG_PASSWORD, password);
        if (this.version != null) props.setProperty(TAG_CQL_VERSION, version);
        if (this.consistency != null) props.setProperty(TAG_CONSISTENCY_LEVEL, consistency);
        if (this.localDataCenter != null) props.setProperty(TAG_LOCAL_DATACENTER, localDataCenter);

        final String url = PROTOCOL + createSubName(props);
        return (CassandraConnection) DriverManager.getConnection(url, props);
    }

    public int getLoginTimeout() {
        return DriverManager.getLoginTimeout();
    }

    public PrintWriter getLogWriter() {
        return DriverManager.getLogWriter();
    }

    public void setLoginTimeout(final int timeout) {
        DriverManager.setLoginTimeout(timeout);
    }

    public void setLogWriter(final PrintWriter writer) {
        DriverManager.setLogWriter(writer);
    }

    public boolean isWrapperFor(final Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

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
