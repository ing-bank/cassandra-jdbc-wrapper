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
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.google.common.collect.Maps;
import com.ing.data.cassandra.jdbc.codec.BigintToBigDecimalCodec;
import com.ing.data.cassandra.jdbc.codec.DecimalToDoubleCodec;
import com.ing.data.cassandra.jdbc.codec.FloatToDoubleCodec;
import com.ing.data.cassandra.jdbc.codec.IntToLongCodec;
import com.ing.data.cassandra.jdbc.codec.LongToIntCodec;
import com.ing.data.cassandra.jdbc.codec.SmallintToIntCodec;
import com.ing.data.cassandra.jdbc.codec.TimestampToLongCodec;
import com.ing.data.cassandra.jdbc.codec.TinyintToIntCodec;
import com.ing.data.cassandra.jdbc.codec.VarintToIntCodec;
import com.ing.data.cassandra.jdbc.optionset.Default;
import com.ing.data.cassandra.jdbc.optionset.OptionSet;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import static com.ing.data.cassandra.jdbc.CassandraResultSet.DEFAULT_CONCURRENCY;
import static com.ing.data.cassandra.jdbc.CassandraResultSet.DEFAULT_HOLDABILITY;
import static com.ing.data.cassandra.jdbc.CassandraResultSet.DEFAULT_TYPE;
import static com.ing.data.cassandra.jdbc.Utils.ALWAYS_AUTOCOMMIT;
import static com.ing.data.cassandra.jdbc.Utils.BAD_TIMEOUT;
import static com.ing.data.cassandra.jdbc.Utils.NO_INTERFACE;
import static com.ing.data.cassandra.jdbc.Utils.NO_TRANSACTIONS;
import static com.ing.data.cassandra.jdbc.Utils.PROTOCOL;
import static com.ing.data.cassandra.jdbc.Utils.TAG_ACTIVE_CQL_VERSION;
import static com.ing.data.cassandra.jdbc.Utils.TAG_COMPLIANCE_MODE;
import static com.ing.data.cassandra.jdbc.Utils.TAG_CONSISTENCY_LEVEL;
import static com.ing.data.cassandra.jdbc.Utils.TAG_CQL_VERSION;
import static com.ing.data.cassandra.jdbc.Utils.TAG_DATABASE_NAME;
import static com.ing.data.cassandra.jdbc.Utils.TAG_DEBUG;
import static com.ing.data.cassandra.jdbc.Utils.TAG_USER;
import static com.ing.data.cassandra.jdbc.Utils.WAS_CLOSED_CONN;
import static com.ing.data.cassandra.jdbc.Utils.createSubName;

/**
 * Cassandra connection: implementation class for {@link Connection} to create a JDBC connection to a Cassandra cluster.
 */
public class CassandraConnection extends AbstractConnection implements Connection {

    /**
     * The database product name.
     */
    public static final String DB_PRODUCT_NAME = "Cassandra";
    /**
     * The default CQL language version supported by the driver.
     */
    public static final String DEFAULT_CQL_VERSION = "3.0.0";

    // Minimal Apache Cassandra version supported by the DataStax Java Driver for Apache Cassandra on top which this
    // wrapper is built.
    /**
     * Minimal Apache Cassandra major version supported by the DataStax Java Driver for Apache Cassandra.
     */
    public static volatile int dbMajorVersion = 2;
    /**
     * Minimal Apache Cassandra minor version supported by the DataStax Java Driver for Apache Cassandra.
     */
    public static volatile int dbMinorVersion = 1;
    /**
     * Minimal Apache Cassandra patch version supported by the DataStax Java Driver for Apache Cassandra.
     */
    public static volatile int dbPatchVersion = 0;

    private static final Logger LOG = LoggerFactory.getLogger(CassandraConnection.class);
    private static final boolean AUTO_COMMIT_DEFAULT = true;

    /**
     * The username used by the connection.
     */
    protected String username;
    /**
     * The connection URL.
     */
    protected String url;

    private final SessionHolder sessionHolder;
    private final Session cSession;
    private final Properties connectionProperties;
    private final Metadata metadata;
    // Set of all the statements that have been created by this connection.
    @SuppressWarnings("SortedCollectionWithNonComparableKeys")
    private final Set<Statement> statements = new ConcurrentSkipListSet<>();
    private final ConcurrentMap<String, CassandraPreparedStatement> preparedStatements = Maps.newConcurrentMap();
    private final ConsistencyLevel defaultConsistencyLevel;
    private String currentKeyspace;
    private final boolean debugMode;
    private Properties clientInfo;
    private volatile boolean isClosed;
    private final OptionSet optionSet;

    /**
     * Instantiates a new JDBC connection to a Cassandra cluster.
     *
     * @param sessionHolder The session holder.
     * @throws SQLException if something went wrong during the initialisation of the connection.
     */
    public CassandraConnection(final SessionHolder sessionHolder) throws SQLException {
        this.sessionHolder = sessionHolder;
        final Properties sessionProperties = sessionHolder.properties;
        final DriverExecutionProfile defaultConfigProfile =
            sessionHolder.session.getContext().getConfig().getDefaultProfile();
        this.debugMode = Boolean.TRUE.toString().equals(sessionProperties.getProperty(TAG_DEBUG, StringUtils.EMPTY));
        this.connectionProperties = (Properties) sessionProperties.clone();
        this.clientInfo = new Properties();
        this.url = PROTOCOL.concat(createSubName(sessionProperties));
        this.currentKeyspace = sessionProperties.getProperty(TAG_DATABASE_NAME);
        this.optionSet = lookupOptionSet(sessionProperties.getProperty(TAG_COMPLIANCE_MODE));
        this.username = sessionProperties.getProperty(TAG_USER,
            defaultConfigProfile.getString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, StringUtils.EMPTY));
        final String cqlVersion = sessionProperties.getProperty(TAG_CQL_VERSION, DEFAULT_CQL_VERSION);
        this.connectionProperties.setProperty(TAG_ACTIVE_CQL_VERSION, cqlVersion);
        this.defaultConsistencyLevel = DefaultConsistencyLevel.valueOf(
            sessionProperties.getProperty(TAG_CONSISTENCY_LEVEL,
                defaultConfigProfile.getString(DefaultDriverOption.REQUEST_CONSISTENCY,
                    ConsistencyLevel.LOCAL_ONE.name())));
        this.cSession = sessionHolder.session;
        this.metadata = cSession.getMetadata();

        final List<SessionHolder> l = new ArrayList<>();
        l.stream().map(s -> s.session).collect(Collectors.toList());

        LOG.info("Connected to cluster: {}, with session: {}", getCatalog(), cSession.getName());
        metadata.getNodes().forEach(
            (uuid, node) -> LOG.info("Datacenter: {}; Host: {}; Rack: {}", node.getDatacenter(),
                node.getEndPoint().resolve(), node.getRack())
        );

        // TODO this is shared among all Connections, what if they belong to different clusters?
        metadata.getNodes().entrySet().stream().findFirst().ifPresent(entry -> {
            final Version cassandraVersion = entry.getValue().getCassandraVersion();
            if (cassandraVersion != null) {
                CassandraConnection.dbMajorVersion = cassandraVersion.getMajor();
                CassandraConnection.dbMinorVersion = cassandraVersion.getMinor();
                CassandraConnection.dbPatchVersion = cassandraVersion.getPatch();
                LOG.info("Node: {} runs Cassandra v.{}", entry.getValue().getEndPoint().resolve(), cassandraVersion);
            }
        });
    }

    /**
     * Instantiates a new JDBC connection to a Cassandra cluster using preexisting session.
     *
     * @param cSession                The session to use.
     * @param currentKeyspace         The keyspace to use.
     * @param defaultConsistencyLevel The default consistency level.
     * @param debugMode               Debug mode flag.
     * @param optionSet               The compliance mode option set to use.
     */
    public CassandraConnection(final Session cSession, final String currentKeyspace,
                               final ConsistencyLevel defaultConsistencyLevel,
                               final boolean debugMode, final OptionSet optionSet) {
        this.sessionHolder = null;
        this.connectionProperties = new Properties();

        if (optionSet == null) {
            this.optionSet = lookupOptionSet(null);
        } else {
            this.optionSet = optionSet;
        }

        this.currentKeyspace = currentKeyspace;
        this.cSession = cSession;
        this.metadata = cSession.getMetadata();
        this.defaultConsistencyLevel = defaultConsistencyLevel;
        this.debugMode = debugMode;
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

        codecs.forEach(codec -> ((DefaultCodecRegistry) cSession.getContext().getCodecRegistry()).register(codec));
    }

    /**
     * Checks whether the connection is closed.
     *
     * @throws SQLException if the connection is closed.
     * @throws SQLNonTransientConnectionException if the connection is closed.
     */
    private void checkNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLNonTransientConnectionException(WAS_CLOSED_CONN);
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        // This implementation does not support the collection of warnings so clearing is a no-op but it still throws an
        // exception when called on a closed connection.
        checkNotClosed();
    }

    @Override
    public void close() throws SQLException {
        if (sessionHolder != null) {
            this.sessionHolder.release();
        }
        this.isClosed = true;
    }

    @Override
    public void commit() throws SQLException {
        // Note that Cassandra only supports auto-commit mode, so this is a no-op but it still throws an exception when
        // called on a closed connection.
        checkNotClosed();
    }

    @Override
    public java.sql.Statement createStatement() throws SQLException {
        checkNotClosed();
        final Statement statement = new CassandraStatement(this);
        this.statements.add(statement);
        return statement;
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
        checkNotClosed();
        final Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency);
        this.statements.add(statement);
        return statement;
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency,
                                     final int resultSetHoldability) throws SQLException {
        checkNotClosed();
        final Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency,
            resultSetHoldability);
        this.statements.add(statement);
        return statement;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkNotClosed();
        return AUTO_COMMIT_DEFAULT;
    }

    @Override
    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        // Note that Cassandra only supports auto-commit mode, so this is a no-op but it still throws an exception when
        // called on a closed connection.
        checkNotClosed();
    }

    @Override
    public String getCatalog() throws SQLException {
        checkNotClosed();
        return optionSet.getCatalog();
    }

    @Override
    public void setCatalog(final String catalog) throws SQLException {
        // The rationale is there are no catalog name to set in this implementation, so we are "silently ignoring" the
        // request but it still throws an exception when called on closed connection.
        checkNotClosed();
    }

    /**
     * Gets a {@link Properties} object listing the properties of this connection.
     *
     * @return The properties of this connection.
     */
    public Properties getConnectionProperties() {
        return this.connectionProperties;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkNotClosed();
        return this.clientInfo;
    }

    @Override
    public String getClientInfo(final String label) throws SQLException {
        checkNotClosed();
        return clientInfo.getProperty(label);
    }

    @Override
    public void setClientInfo(final Properties properties) {
        // we don't use them but we will happily collect them for now...
        if (properties != null) {
            this.clientInfo = properties;
        }
    }

    @Override
    public void setClientInfo(final String key, final String value) {
        // we don't use them but we will happily collect them for now...
        clientInfo.setProperty(key, value);
    }

    /**
     * Gets the metadata of the Cassandra cluster used by this connection.
     *
     * @return The metadata of the Cassandra cluster used by this connection.
     */
    public Metadata getClusterMetadata() {
        return this.metadata;
    }

    /**
     * Gets whether the debug mode is active on this connection.
     *
     * @return {@code true} if the debug mode is active on this connection, {@code false} otherwise.
     */
    public boolean isDebugMode() {
        return this.debugMode;
    }

    /**
     * Gets the default consistency level applied to this connection.
     *
     * @return The default consistency level applied to this connection.
     */
    public ConsistencyLevel getDefaultConsistencyLevel() {
        return this.defaultConsistencyLevel;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkNotClosed();
        // The rationale is there are really no commits in Cassandra so no boundary.
        return DEFAULT_HOLDABILITY;
    }

    @Override
    public void setHoldability(final int holdability) throws SQLException {
        // The rationale is there are no holdability to set in this implementation, so we are "silently ignoring" the
        // request but it still throws an exception when called on closed connection.
        checkNotClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkNotClosed();
        return new CassandraDatabaseMetaData(this);
    }

    @Override
    public String getSchema() throws SQLException {
        checkNotClosed();
        return this.currentKeyspace;
    }

    @Override
    public void setSchema(final String schema) throws SQLException {
        checkNotClosed();
        this.currentKeyspace = schema;
    }

    /**
     * Gets the CQL session used to send requests to the Cassandra cluster.
     *
     * @return The CQL session.
     */
    public Session getSession() {
        return this.cSession;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkNotClosed();
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public void setTransactionIsolation(final int level) throws SQLException {
        checkNotClosed();
        if (level != Connection.TRANSACTION_NONE) {
            throw new SQLFeatureNotSupportedException(NO_TRANSACTIONS);
        }
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        final HashMap<String, Class<?>> typeMap = new HashMap<>();
        LOG.info("Current keyspace: " + currentKeyspace);
        this.metadata.getKeyspace(currentKeyspace)
            .ifPresent(keyspaceMetadata ->
                keyspaceMetadata.getUserDefinedTypes().forEach((cqlIdentifier, userDefinedType) ->
                    typeMap.put(cqlIdentifier.asInternal(), userDefinedType.getClass()))
            );

        return typeMap;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // The rationale is there are no warnings to return in this implementation, so we return null or throw an
        // exception when called on closed connection.
        checkNotClosed();
        return null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkNotClosed();
        // All the connections are read/write in the Cassandra implementation, so always return false.
        return false;
    }

    @Override
    public void setReadOnly(final boolean readOnly) throws SQLException {
        // The rationale is all the connections are read/write in the Cassandra implementation, so we are "silently
        // ignoring" the request but it still throws an exception when called on closed connection.
        checkNotClosed();
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
        // The rationale is there is no distinction between grammars in this implementation, so we just return the
        // input argument or throw an exception when called on closed connection.
        checkNotClosed();
        return sql;
    }

    @Override
    public CassandraPreparedStatement prepareStatement(final String cql) throws SQLException {
        CassandraPreparedStatement preparedStatement = this.preparedStatements.get(cql);
        if (preparedStatement == null) {
            // Statement didn't exist, create it and put it to the map of prepared statements.
            preparedStatement = this.preparedStatements.putIfAbsent(cql, prepareStatement(cql, DEFAULT_TYPE,
                DEFAULT_CONCURRENCY, DEFAULT_HOLDABILITY));
            if (preparedStatement == null) {
                // Statement has already been created by another thread, so we'll just get it.
                return this.preparedStatements.get(cql);
            }
        }

        return preparedStatement;
    }

    @Override
    public CassandraPreparedStatement prepareStatement(final String cql, final int resultSetType,
                                                       final int resultSetConcurrency) throws SQLException {
        return prepareStatement(cql, resultSetType, resultSetConcurrency, DEFAULT_HOLDABILITY);
    }

    @Override
    public CassandraPreparedStatement prepareStatement(final String cql, final int resultSetType,
                                                       final int resultSetConcurrency,
                                                       final int resultSetHoldability) throws SQLException {
        checkNotClosed();
        final CassandraPreparedStatement statement = new CassandraPreparedStatement(this, cql, resultSetType,
            resultSetConcurrency, resultSetHoldability);
        this.statements.add(statement);
        return statement;
    }

    @Override
    public void rollback() throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    /**
     * Removes a statement from the open statements list.
     *
     * @param statement The statement.
     * @return {@code true} if the statement has successfully been removed, {@code false} otherwise.
     */
    @SuppressWarnings("UnusedReturnValue")
    protected boolean removeStatement(final Statement statement) {
        return this.statements.remove(statement);
    }

    @Override
    public String toString() {
        return "CassandraConnection [connectionProperties=" + connectionProperties + "]";
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    /**
     * Gets the compliance mode option set used for the connection.
     *
     * @return The compliance mode option set used for the connection.
     */
    public OptionSet getOptionSet() {
        return optionSet;
    }

    private OptionSet lookupOptionSet(final String property) {
        final ServiceLoader<OptionSet> loader = ServiceLoader.load(OptionSet.class);
        final Iterator<OptionSet> iterator = loader.iterator();
        while (iterator.hasNext()) {
            final OptionSet optionSet = iterator.next();
            if (optionSet.getClass().getSimpleName().equalsIgnoreCase(property)) {
                optionSet.setConnection(this);
                return optionSet;
            }
        }
        final OptionSet optionSet = new Default();
        optionSet.setConnection(this);
        return optionSet;
    }

}
