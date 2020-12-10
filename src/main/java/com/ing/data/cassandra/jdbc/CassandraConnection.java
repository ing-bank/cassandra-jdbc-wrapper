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
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import static com.ing.data.cassandra.jdbc.CassandraResultSet.DEFAULT_CONCURRENCY;
import static com.ing.data.cassandra.jdbc.CassandraResultSet.DEFAULT_HOLDABILITY;
import static com.ing.data.cassandra.jdbc.CassandraResultSet.DEFAULT_TYPE;
import static com.ing.data.cassandra.jdbc.Utils.ALWAYS_AUTOCOMMIT;
import static com.ing.data.cassandra.jdbc.Utils.BAD_TIMEOUT;
import static com.ing.data.cassandra.jdbc.Utils.NO_INTERFACE;
import static com.ing.data.cassandra.jdbc.Utils.NO_TRANSACTIONS;
import static com.ing.data.cassandra.jdbc.Utils.PROTOCOL;
import static com.ing.data.cassandra.jdbc.Utils.TAG_ACTIVE_CQL_VERSION;
import static com.ing.data.cassandra.jdbc.Utils.TAG_CONSISTENCY_LEVEL;
import static com.ing.data.cassandra.jdbc.Utils.TAG_CQL_VERSION;
import static com.ing.data.cassandra.jdbc.Utils.TAG_DATABASE_NAME;
import static com.ing.data.cassandra.jdbc.Utils.TAG_DEBUG;
import static com.ing.data.cassandra.jdbc.Utils.TAG_USER;
import static com.ing.data.cassandra.jdbc.Utils.WAS_CLOSED_CON;
import static com.ing.data.cassandra.jdbc.Utils.createSubName;

/**
 * Implementation class for {@link Connection}.
 */
public class CassandraConnection extends AbstractConnection implements Connection {
    private static final Logger log = LoggerFactory.getLogger(CassandraConnection.class);

    public static volatile int DB_MAJOR_VERSION = 1;
    public static volatile int DB_MINOR_VERSION = 2;
    public static volatile int DB_REVISION = 2;
    public static final String DB_PRODUCT_NAME = "Cassandra";
    public static final String DEFAULT_CQL_VERSION = "3.0.0";
    public ConcurrentMap<String, CassandraPreparedStatement> preparedStatements = Maps.newConcurrentMap();

    private final boolean autoCommit = true;

    private final int transactionIsolation = Connection.TRANSACTION_NONE;
    private final SessionHolder sessionHolder;

    /**
     * Connection Properties
     */
    private final Properties connectionProps;

    /**
     * Client Info Properties (currently unused)
     */
    private Properties clientInfo;

    /**
     * Set of all Statements that have been created by this connection
     */
    private final Set<Statement> statements = new ConcurrentSkipListSet<>();

    private final Session cSession;

    protected int numFailures = 0;
    protected String username;
    protected String url;
    public String cluster;
    protected String currentKeyspace;
    protected TreeSet<String> hostListPrimary;
    protected TreeSet<String> hostListBackup;
    int majorCqlVersion;
    private Metadata metadata;
    public boolean debugMode;
    private volatile boolean isClosed;

    public ConsistencyLevel defaultConsistencyLevel;

    /**
     * Instantiates a new CassandraConnection.
     *
     * @param sessionHolder The session holder.
     * @throws SQLException
     */
    public CassandraConnection(final SessionHolder sessionHolder) throws SQLException {
        this.sessionHolder = sessionHolder;
        final Properties props = sessionHolder.properties;

        debugMode = Boolean.TRUE.toString().equals(props.getProperty(TAG_DEBUG, StringUtils.EMPTY));
        hostListPrimary = new TreeSet<>();
        hostListBackup = new TreeSet<>();
        connectionProps = (Properties) props.clone();
        clientInfo = new Properties();
        url = PROTOCOL + createSubName(props);
        currentKeyspace = props.getProperty(TAG_DATABASE_NAME);
        username = props.getProperty(TAG_USER, StringUtils.EMPTY);
        final String version = props.getProperty(TAG_CQL_VERSION, DEFAULT_CQL_VERSION);
        connectionProps.setProperty(TAG_ACTIVE_CQL_VERSION, version);
        majorCqlVersion = getMajor(version);
        defaultConsistencyLevel = DefaultConsistencyLevel.valueOf(props.getProperty(TAG_CONSISTENCY_LEVEL,
            ConsistencyLevel.ONE.name()));

        cSession = sessionHolder.session;

        metadata = cSession.getMetadata();
        log.info(String.format("Connected to cluster: %s, with session: %s", getCatalog(), cSession.getName()));
        metadata.getNodes().forEach((uuid, node) -> {
            log.info(String.format("Datacenter: %s; Host: %s; Rack: %s",
                node.getDatacenter(), node.getEndPoint().resolve(), node.getRack()));
        });

        // TODO this is shared among all Connections, what if they belong to different clusters?
        metadata.getNodes().entrySet().stream().findFirst().ifPresent(entry -> {
            final Version cassandraVersion = entry.getValue().getCassandraVersion();
            if (cassandraVersion != null) {
                CassandraConnection.DB_MAJOR_VERSION = cassandraVersion.getMajor();
                CassandraConnection.DB_MINOR_VERSION = cassandraVersion.getMinor();
                CassandraConnection.DB_REVISION = cassandraVersion.getPatch();
            }
        });
    }

    // get the Major portion of a string like : Major.minor.patch where 2 is the default
    @SuppressWarnings("boxing")
    private int getMajor(final String version) {
        int major = 0;
        final String[] parts = version.split("\\.");
        try {
            major = Integer.parseInt(parts[0]);
        } catch (final Exception e) {
            major = 2;
        }
        return major;
    }

    private void checkNotClosed() throws SQLException {
        if (isClosed()) throw new SQLNonTransientConnectionException(WAS_CLOSED_CON);
    }

    @Override
    public void clearWarnings() throws SQLException {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    /**
     * On close of connection.
     */
    @Override
    public void close() throws SQLException {
        sessionHolder.release();
        isClosed = true;
    }

    @Override
    public void commit() throws SQLException {
        checkNotClosed();
        //throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    @Override
    public java.sql.Statement createStatement() throws SQLException {
        checkNotClosed();
        final Statement statement = new CassandraStatement(this);

        statements.add(statement);
        return statement;
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
        checkNotClosed();
        final Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency);
        statements.add(statement);
        return statement;
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency,
                                     final int resultSetHoldability) throws SQLException {
        checkNotClosed();
        final Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency,
            resultSetHoldability);
        statements.add(statement);
        return statement;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkNotClosed();
        return autoCommit;
    }

    public Properties getConnectionProps() {
        return connectionProps;
    }

    @Override
    public String getCatalog() throws SQLException {
        checkNotClosed();

        // It requires a query to table system.local since DataStax driver 4+.
        // If the query fails, return null.
        try (final Statement stmt = createStatement()) {
            final ResultSet rs = stmt.executeQuery("SELECT cluster_name FROM system.local");
            if (rs.next()) {
                return rs.getString("cluster_name");
            }
        } catch (final SQLException e) {
            log.warn("Unable to retrieve the cluster name.", e);
            return null;
        }

        return null;
    }

    @Override
    public void setSchema(final String schema) throws SQLException {
        checkNotClosed();
        currentKeyspace = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        checkNotClosed();
        return currentKeyspace;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkNotClosed();
        return clientInfo;
    }

    @Override
    public String getClientInfo(final String label) throws SQLException {
        checkNotClosed();
        return clientInfo.getProperty(label);
    }

    @Override
    public int getHoldability() throws SQLException {
        checkNotClosed();
        // the rationale is there are really no commits in Cassandra so no boundary...
        return DEFAULT_HOLDABILITY;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkNotClosed();
        return new CassandraDatabaseMetaData(this);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkNotClosed();
        return transactionIsolation;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkNotClosed();
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkNotClosed();
        return false;
    }

    @Override
    public boolean isValid(final int timeout) throws SQLTimeoutException {
        if (timeout < 0) {
            throw new SQLTimeoutException(BAD_TIMEOUT);
        }

        // TODO: set timeout

        return true;
    }

    @Override
    public boolean isWrapperFor(final Class<?> arg0) {
        return false;
    }

    @Override
    public String nativeSQL(final String sql) throws SQLException {
        checkNotClosed();
        // the rationale is there are no distinction between grammars in this implementation...
        // so we are just return the input argument
        return sql;
    }

    @Override
    public CassandraPreparedStatement prepareStatement(final String cql) throws SQLException {
        CassandraPreparedStatement prepStmt = preparedStatements.get(cql);
        if (prepStmt == null) {
            // Statement didn't exist
            prepStmt = preparedStatements.putIfAbsent(cql, prepareStatement(cql, DEFAULT_TYPE, DEFAULT_CONCURRENCY,
                DEFAULT_HOLDABILITY));
            if (prepStmt == null) {
                // Statement has already been created by another thread, so we'll just get it
                return preparedStatements.get(cql);
            }
        }

        return prepStmt;
    }

    @Override
    public CassandraPreparedStatement prepareStatement(final String cql, final int rsType) throws SQLException {
        return prepareStatement(cql, rsType, DEFAULT_CONCURRENCY, DEFAULT_HOLDABILITY);
    }

    @Override
    public CassandraPreparedStatement prepareStatement(final String cql, final int rsType, final int rsConcurrency)
        throws SQLException {
        return prepareStatement(cql, rsType, rsConcurrency, DEFAULT_HOLDABILITY);
    }

    @Override
    public CassandraPreparedStatement prepareStatement(final String cql, final int rsType, final int rsConcurrency,
                                                       final int rsHoldability) throws SQLException {
        checkNotClosed();
        final CassandraPreparedStatement statement = new CassandraPreparedStatement(this, cql, rsType, rsConcurrency,
            rsHoldability);
        statements.add(statement);
        return statement;
    }

    @Override
    public void rollback() throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    @Override
    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        checkNotClosed();
        //if (!autoCommit) throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    @Override
    public void setCatalog(final String arg0) throws SQLException {
        checkNotClosed();
        // the rationale is there are no catalog name to set in this implementation...
        // so we are "silently ignoring" the request
    }

    @Override
    public void setClientInfo(final Properties props) {
        // we don't use them but we will happily collect them for now...
        if (props != null) {
            clientInfo = props;
        }
    }

    @Override
    public void setClientInfo(final String key, final String value) {
        // we don't use them but we will happily collect them for now...
        clientInfo.setProperty(key, value);
    }

    @Override
    public void setHoldability(final int arg0) throws SQLException {
        checkNotClosed();
        // the rationale is there are no holdability to set in this implementation...
        // so we are "silently ignoring" the request
    }

    @Override
    public void setReadOnly(final boolean arg0) throws SQLException {
        checkNotClosed();
        // the rationale is all connections are read/write in the Cassandra implementation...
        // so we are "silently ignoring" the request
    }

    @Override
    public void setTransactionIsolation(final int level) throws SQLException {
        checkNotClosed();
        if (level != Connection.TRANSACTION_NONE) throw new SQLFeatureNotSupportedException(NO_TRANSACTIONS);
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    /**
     * Removes a statement from the open statements list.
     *
     * @param statement The statement.
     * @return {@code true} if the statement has successfully been removed, {@code false} otherwise.
     */
    protected boolean removeStatement(final Statement statement) {
        return statements.remove(statement);
    }

    public String toString() {
        return "CassandraConnection [connectionProps=" + connectionProps + "]";
    }

    public Session getSession() {
        return this.cSession;
    }

    public Metadata getClusterMetadata() {
        return metadata;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        final HashMap<String, Class<?>> typeMap = new HashMap<>();
        log.info("Current keyspace : " + currentKeyspace);
        this.metadata.getKeyspace(currentKeyspace).ifPresent(keyspaceMetadata -> {
            keyspaceMetadata.getUserDefinedTypes().forEach(((cqlIdentifier, userDefinedType) -> {
                typeMap.put(cqlIdentifier.asInternal(), userDefinedType.getClass());
            }));
        });

        return typeMap;
    }

}
