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
import com.ing.data.cassandra.jdbc.optionset.Default;
import com.ing.data.cassandra.jdbc.optionset.OptionSet;
import com.ing.data.cassandra.jdbc.utils.ArrayImpl;
import com.ing.data.cassandra.jdbc.utils.BlobImpl;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Array;
import java.sql.Blob;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ing.data.cassandra.jdbc.CassandraResultSet.DEFAULT_CONCURRENCY;
import static com.ing.data.cassandra.jdbc.CassandraResultSet.DEFAULT_HOLDABILITY;
import static com.ing.data.cassandra.jdbc.CassandraResultSet.DEFAULT_TYPE;
import static com.ing.data.cassandra.jdbc.utils.AwsUtil.listAwsKeyspacesHosts;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.listPreconfiguredCodecs;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.safelyRegisterCodecs;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.toStringWithoutSensitiveValues;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.ALWAYS_AUTOCOMMIT;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_TIMEOUT;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NO_TRANSACTIONS;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.WAS_CLOSED_CONN;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.PROTOCOL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_ACTIVE_PROFILE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_COMPLIANCE_MODE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONSISTENCY_LEVEL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_DATABASE_NAME;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_DEBUG;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_FETCH_SIZE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_SERIAL_CONSISTENCY_LEVEL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_USER;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.createSubName;
import static com.ing.data.cassandra.jdbc.utils.WarningConstants.INVALID_FETCH_SIZE_PARAMETER;
import static com.ing.data.cassandra.jdbc.utils.WarningConstants.INVALID_PROFILE_NAME;
import static java.util.concurrent.Executors.newCachedThreadPool;

/**
 * Cassandra connection: implementation class for {@link Connection} to create a JDBC connection to a Cassandra
 * cluster.
 */
@Slf4j
public class CassandraConnection extends AbstractConnection implements Connection {

    // Minimal Apache Cassandra version supported by the Java Driver for Apache Cassandra® on top which this wrapper is
    // built.
    // If available, the effective version run by the node on which the connection is established will override these
    // values.
    /**
     * Minimal Apache Cassandra major version supported by the Java Driver for Apache Cassandra®.
     */
    public static volatile int dbMajorVersion = 2;
    /**
     * Minimal Apache Cassandra minor version supported by the Java Driver for Apache Cassandra®.
     */
    public static volatile int dbMinorVersion = 1;
    /**
     * Minimal Apache Cassandra patch version supported by the Java Driver for Apache Cassandra®.
     */
    public static volatile int dbPatchVersion = 0;

    /**
     * The default fetch size when it's not configured neither in JDBC URL nor in a configuration file and the default
     * fetch size (request page size) cannot be retrieved from the configuration profile.
     */
    protected static final int FALLBACK_FETCH_SIZE = 100;

    private static final boolean AUTO_COMMIT_DEFAULT = true;

    /**
     * The username used by the connection.
     */
    protected String username;
    /**
     * The connection URL.
     */
    protected String url;
    /**
     * Whether the connection is bound to an Amazon Keyspaces database.
     */
    private Boolean connectedToAmazonKeyspaces;
    /**
     * The properties of this connection.
     */
    @Getter
    private final Properties connectionProperties;
    /**
     * The consistency level applied to this connection.
     */
    @Getter
    @Setter
    private ConsistencyLevel consistencyLevel;
    /**
     * The serial consistency level applied to this connection.
     */
    @Getter
    @Setter
    private ConsistencyLevel serialConsistencyLevel;
    /**
     * The default fetch size applied to this connection.
     */
    @Getter
    private int defaultFetchSize = FALLBACK_FETCH_SIZE;
    /**
     * Whether the debug mode is active on this connection: {@code true} if the debug mode is active on this
     * connection, {@code false} otherwise.
     */
    @Getter
    private final boolean debugMode;
    /**
     * The compliance mode option set used for the connection.
     */
    @Getter
    private OptionSet optionSet = new Default();
    /**
     * The active execution profile.
     */
    @Getter
    private DriverExecutionProfile activeExecutionProfile;

    private final SessionHolder sessionHolder;
    private final Session cSession;
    private final Metadata metadata;
    // Set of all the statements that have been created by this connection.
    @SuppressWarnings("SortedCollectionWithNonComparableKeys")
    private final Set<Statement> statements = new ConcurrentSkipListSet<>();
    private final ConcurrentMap<String, CassandraPreparedStatement> preparedStatements = new ConcurrentHashMap<>();
    private String currentKeyspace;
    private Properties clientInfo;
    private volatile boolean isClosed;
    private DriverExecutionProfile lastUsedExecutionProfile;

    /**
     * Instantiates a new JDBC connection to a Cassandra cluster.
     *
     * @param sessionHolder The session holder.
     * @throws SQLException if something went wrong during the initialisation of the connection.
     */
    CassandraConnection(final SessionHolder sessionHolder) throws SQLException {
        this.sessionHolder = sessionHolder;
        final Properties sessionProperties = sessionHolder.properties;
        this.cSession = sessionHolder.session;
        this.metadata = this.cSession.getMetadata();
        final String executionProfile = sessionProperties.getProperty(TAG_ACTIVE_PROFILE,
            DriverExecutionProfile.DEFAULT_NAME);
        setActiveExecutionProfile(executionProfile);
        this.debugMode = Boolean.TRUE.toString().equals(sessionProperties.getProperty(TAG_DEBUG, StringUtils.EMPTY));
        this.connectionProperties = (Properties) sessionProperties.clone();
        this.clientInfo = new Properties();
        this.url = PROTOCOL.concat(createSubName(sessionProperties));
        this.currentKeyspace = sessionProperties.getProperty(TAG_DATABASE_NAME);
        this.optionSet = lookupOptionSet(sessionProperties.getProperty(TAG_COMPLIANCE_MODE));
        this.username = sessionProperties.getProperty(TAG_USER,
            this.activeExecutionProfile.getString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, StringUtils.EMPTY));
        this.consistencyLevel = DefaultConsistencyLevel.valueOf(
            sessionProperties.getProperty(TAG_CONSISTENCY_LEVEL,
                this.activeExecutionProfile.getString(DefaultDriverOption.REQUEST_CONSISTENCY,
                    ConsistencyLevel.LOCAL_ONE.name())));
        this.serialConsistencyLevel = DefaultConsistencyLevel.valueOf(
            sessionProperties.getProperty(TAG_SERIAL_CONSISTENCY_LEVEL,
                this.activeExecutionProfile.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY,
                    ConsistencyLevel.SERIAL.name())));

        final int fetchSizeFromProfile = this.activeExecutionProfile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE,
            FALLBACK_FETCH_SIZE);
        final String fetchSizeParameter = sessionProperties.getProperty(TAG_FETCH_SIZE);
        try {
            if (fetchSizeParameter == null) {
                this.defaultFetchSize = fetchSizeFromProfile;
            } else {
                this.defaultFetchSize = Integer.parseInt(fetchSizeParameter);
            }
        } catch (final NumberFormatException e) {
            log.warn(INVALID_FETCH_SIZE_PARAMETER, fetchSizeParameter, fetchSizeFromProfile);
            this.defaultFetchSize = fetchSizeFromProfile;
        }

        final String clusterName = Objects.toString(getCatalog(), "<not available>");
        log.info("Connected to cluster: {}, with session: {}", clusterName, this.cSession.getName());
        this.metadata.getNodes().forEach(
            (uuid, node) -> log.info("Datacenter: {}; Host: {}; Rack: {}", node.getDatacenter(),
                node.getEndPoint().resolve(), node.getRack())
        );

        this.metadata.getNodes().entrySet().stream().findFirst().ifPresent(entry -> {
            final Version cassandraVersion = entry.getValue().getCassandraVersion();
            if (cassandraVersion != null) {
                CassandraConnection.dbMajorVersion = cassandraVersion.getMajor();
                CassandraConnection.dbMinorVersion = cassandraVersion.getMinor();
                CassandraConnection.dbPatchVersion = cassandraVersion.getPatch();
                log.info("Node: {} runs Cassandra v.{}", entry.getValue().getEndPoint().resolve(), cassandraVersion);
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
     * @param defaultExecutionProfile The name of the default execution profile to use.
     */
    public CassandraConnection(final Session cSession,
                               final String currentKeyspace,
                               final ConsistencyLevel defaultConsistencyLevel,
                               final boolean debugMode,
                               final OptionSet optionSet,
                               final String defaultExecutionProfile) {
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
        this.consistencyLevel = defaultConsistencyLevel;
        this.serialConsistencyLevel = ConsistencyLevel.SERIAL;
        this.debugMode = debugMode;
        setActiveExecutionProfile(Objects.toString(defaultExecutionProfile, DriverExecutionProfile.DEFAULT_NAME));
        safelyRegisterCodecs(cSession, listPreconfiguredCodecs());
    }

    /**
     * Checks whether the current connection is not bound to an Amazon Keyspaces instance.
     * <p>
     *     The main purpose of this method is to adapt the values returned by the methods of
     *     {@link CassandraDatabaseMetaData} when the used database is Amazon Keyspaces since this one does not
     *     implement all the Cassandra features. See
     *     <a href="https://docs.aws.amazon.com/keyspaces/latest/devguide/keyspaces-vs-cassandra.html">comparison
     *     between Cassandra and Keyspaces</a>.
     * </p>
     *
     * @return {@code true} if the connection is not bound to Amazon Keyspaces, {@code false} otherwise.
     */
    public boolean isNotConnectedToAmazonKeyspaces() {
        if (this.connectedToAmazonKeyspaces == null) {
            this.connectedToAmazonKeyspaces = false;
            // Valid Amazon Keyspaces endpoints are available here:
            // https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.endpoints.html
            Set<String> connectionUrlOrEndpoints = new HashSet<>();
            if (this.url != null) {
                connectionUrlOrEndpoints.add(this.url);
            } else {
                try {
                    connectionUrlOrEndpoints = this.metadata.getNodes().values().stream()
                        .map(node -> node.getEndPoint().resolve().toString())
                        .collect(Collectors.toSet());
                } catch (final Exception e) {
                    log.debug("Failed to retrieve the contact points of the connection to determine if the connection "
                        + "is bound to AWS: {}", e.getMessage());
                }
            }
            if (connectionUrlOrEndpoints.stream().anyMatch(
                endpoint -> listAwsKeyspacesHosts().stream().anyMatch(endpoint::contains))) {
                this.connectedToAmazonKeyspaces = true;
            } else {
                // Check for the existence of the keyspace 'system_schema_mcs' (specific to Amazon Keyspaces).
                try (final CassandraStatement stmt = (CassandraStatement) this.createStatement()) {
                    stmt.execute(
                        "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'system_schema_mcs'");
                    final ResultSet rs = stmt.getResultSet();
                    this.connectedToAmazonKeyspaces = rs != null && rs.next();
                } catch (final Exception e) {
                    log.debug("Failed to check existing keyspaces to determine if the connection is bound to AWS: {}",
                        e.getMessage());
                }
            }
        }
        return !this.connectedToAmazonKeyspaces;
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
        // This implementation does not support the collection of warnings so clearing is a no-op, but it still throws
        // an exception when called on a closed connection.
        checkNotClosed();
    }

    @Override
    public void close() throws SQLException {
        if (this.sessionHolder != null) {
            this.sessionHolder.release();
        }
        this.isClosed = true;
    }

    @Override
    public void commit() throws SQLException {
        // Note that Cassandra only supports auto-commit mode, so this is a no-op, but it still throws an exception when
        // called on a closed connection.
        checkNotClosed();
    }

    @Override
    public Array createArrayOf(final String typeName, final Object[] elements) throws SQLException {
        checkNotClosed();
        if (elements == null) {
            return new ArrayImpl();
        }
        return new ArrayImpl(List.of(elements), typeName);
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkNotClosed();
        return new BlobImpl();
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
    public Statement createStatement(final int resultSetType,
                                     final int resultSetConcurrency,
                                     final int resultSetHoldability) throws SQLException {
        checkNotClosed();
        final Statement statement =
            new CassandraStatement(this, null, resultSetType, resultSetConcurrency, resultSetHoldability);
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
        // Note that Cassandra only supports auto-commit mode, so this is a no-op, but it still throws an exception when
        // called on a closed connection.
        checkNotClosed();
    }

    @Override
    public String getCatalog() throws SQLException {
        checkNotClosed();
        return this.optionSet.getCatalog();
    }

    @Override
    public void setCatalog(final String catalog) throws SQLException {
        // The rationale is there are no catalog name to set in this implementation, so we are "silently ignoring" the
        // request, but it still throws an exception when called on closed connection.
        checkNotClosed();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkNotClosed();
        return this.clientInfo;
    }

    @Override
    public String getClientInfo(final String label) throws SQLException {
        checkNotClosed();
        return this.clientInfo.getProperty(label);
    }

    @Override
    public void setClientInfo(final Properties properties) {
        if (properties != null) {
            this.clientInfo = properties;
        }
    }

    @Override
    public void setClientInfo(final String key, final String value) {
        this.clientInfo.setProperty(key, value);
    }

    /**
     * Gets the metadata of the Cassandra cluster used by this connection.
     *
     * @return The metadata of the Cassandra cluster used by this connection.
     */
    public Metadata getClusterMetadata() {
        return this.metadata;
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
        // request, but it still throws an exception when called on closed connection.
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
        log.info("Current keyspace: {}", this.currentKeyspace);
        this.metadata.getKeyspace(this.currentKeyspace)
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
        // ignoring" the request, but it still throws an exception when called on closed connection.
        checkNotClosed();
    }

    @Override
    public boolean isValid(final int timeout) throws SQLTimeoutException {
        // Throw an exception if the timeout value is invalid.
        if (timeout < 0) {
            throw new SQLTimeoutException(BAD_TIMEOUT);
        }

        // Return false if the connection is closed.
        if (getSession().isClosed()) {
            return false;
        }

        try (final CassandraStatement stmt = (CassandraStatement) this.createStatement()) {
            // Wait at most the defined timeout duration (if requested) for the successful query execution.
            final ExecutorService stmtExecutor = newCachedThreadPool();
            final Callable<Object> callableStmt = () -> stmt.execute("SELECT key FROM system.local");
            if (timeout != 0) {
                final Future<Object> futureStmtExecution = stmtExecutor.submit(callableStmt);
                try {
                    futureStmtExecution.get(timeout, TimeUnit.SECONDS);
                } catch (final Exception e) {
                    return false;
                } finally {
                    futureStmtExecution.cancel(true);
                }
            } else {
                // Try to execute the query. If this succeeds, then the connection is valid. If it fails (throws an
                // exception), then the connection is not valid.
                callableStmt.call();
            }
        } catch (final Exception e) {
            return false;
        }

        return true;
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
            preparedStatement = this.preparedStatements.putIfAbsent(cql,
                prepareStatement(cql, DEFAULT_TYPE, DEFAULT_CONCURRENCY, DEFAULT_HOLDABILITY));
            if (preparedStatement == null) {
                // Statement has already been created by another thread, so we'll just get it.
                return this.preparedStatements.get(cql);
            }
        }

        return preparedStatement;
    }

    @Override
    public CassandraPreparedStatement prepareStatement(final String cql,
                                                       final int resultSetType,
                                                       final int resultSetConcurrency) throws SQLException {
        return prepareStatement(cql, resultSetType, resultSetConcurrency, DEFAULT_HOLDABILITY);
    }

    @Override
    public CassandraPreparedStatement prepareStatement(final String cql,
                                                       final int resultSetType,
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
        if (this.optionSet.shouldThrowExceptionOnRollback()) {
            throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
        }
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
        return String.format("CassandraConnection [connectionProperties=%s]",
            toStringWithoutSensitiveValues(this.connectionProperties));
    }

    /**
     * Sets the compliance mode option set used for the connection.
     *
     * @param optionSet The compliance mode option set used for the connection.
     */
    public void setOptionSet(final OptionSet optionSet) {
        optionSet.setConnection(this);
        this.optionSet = optionSet;
    }

    private OptionSet lookupOptionSet(final String property) {
        final ServiceLoader<OptionSet> loader = ServiceLoader.load(OptionSet.class);
        for (final OptionSet loadedOptionSet : loader) {
            if (loadedOptionSet.getClass().getSimpleName().equalsIgnoreCase(property)) {
                loadedOptionSet.setConnection(this);
                return loadedOptionSet;
            }
        }
        final OptionSet defaultOptionSet = new Default();
        defaultOptionSet.setConnection(this);
        return defaultOptionSet;
    }

    /**
     * Sets a new execution profile for the current connection.
     * <p>
     *     If the given profile name is invalid, this method has no effect.
     * </p>
     *
     * @param profile The execution profile to set.
     */
    public void setActiveExecutionProfile(final String profile) {
        final DriverExecutionProfile currentProfile = this.activeExecutionProfile;
        try {
            this.activeExecutionProfile = this.cSession.getContext().getConfig().getProfile(profile);
            this.lastUsedExecutionProfile = currentProfile;
        } catch (final IllegalArgumentException e) {
            log.warn(INVALID_PROFILE_NAME, profile);
            if (this.activeExecutionProfile == null) {
                this.activeExecutionProfile = this.cSession.getContext().getConfig().getDefaultProfile();
            }
        }
    }

    /**
     * Restores the last execution profile defined before switching it.
     * <p>
     *     If the execution profile has never been changed previously, this method has no effect.
     * </p>
     */
    public void restoreLastExecutionProfile() {
        if (this.lastUsedExecutionProfile != null) {
            this.activeExecutionProfile = this.lastUsedExecutionProfile;
        }
    }

}
