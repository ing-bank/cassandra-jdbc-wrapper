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
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.internal.core.cql.MultiPageResultSet;
import com.datastax.oss.driver.internal.core.cql.SinglePageResultSet;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import static com.ing.data.cassandra.jdbc.StatementExecutor.EXECUTORS;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_AUTO_GEN;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_CONCURRENCY_RS;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_FETCH_DIR;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_FETCH_SIZE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_HOLD_RS;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_KEEP_RS;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_TYPE_RS;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NO_GEN_KEYS;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NO_MULTIPLE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NO_RESULT_SET;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.TOO_MANY_QUERIES;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.WAS_CLOSED_STMT;
import static org.apache.commons.lang3.StringUtils.countMatches;

/**
 * Cassandra statement: implementation class for {@link Statement}.
 * <p>
 * It also implements {@link CassandraStatementExtras} interface providing extra methods not defined in JDBC API to
 * manage some properties specific to the Cassandra statements (e.g. consistency level).
 * </p>
 */
public class CassandraStatement extends AbstractStatement
    implements CassandraStatementExtras, Comparable<Object>, Statement {

    /**
     * Maximal number of queries executable in a single batch.
     */
    public static final int MAX_ASYNC_QUERIES = 1000;
    /**
     * CQL statements separator: semi-colon ({@code ;}).
     */
    public static final String STATEMENTS_SEPARATOR_REGEX = ";";
    /**
     * The default fetch size.
     */
    protected static final int DEFAULT_FETCH_SIZE = 100;

    private static final Logger LOG = LoggerFactory.getLogger(CassandraStatement.class);

    /**
     * The Cassandra connection.
     */
    protected CassandraConnection connection;
    /**
     * The CQL statement.
     */
    protected String cql;
    /**
     * The list of CQL queries contained into a single batch.
     */
    protected ArrayList<String> batchQueries;
    /**
     * The direction for fetching rows from database. By default, {@link ResultSet#FETCH_FORWARD}.
     */
    protected int fetchDirection = ResultSet.FETCH_FORWARD;
    /**
     * The number of result set rows that is the default fetch size for {@link ResultSet} objects generated from this
     * statement.
     */
    protected int fetchSize;
    /**
     * The maximum number of bytes that can be returned for character and binary column values in a {@link ResultSet}
     * object produced by this statement. By default, there is no limit (0).
     */
    protected int maxFieldSize = 0;
    /**
     * The maximum number of rows that a {@link ResultSet} object produced by this statement. By default, there is no
     * limit (0).
     */
    protected int maxRows = 0;
    /**
     * The result set type for {@link ResultSet} objects generated by this statement.
     */
    protected int resultSetType;
    /**
     * The result set concurrency for {@link ResultSet} objects generated by this statement.
     */
    protected int resultSetConcurrency;
    /**
     * The result set holdability for {@link ResultSet} objects generated by this statement.
     */
    protected int resultSetHoldability;
    /**
     * The current result set for this statement.
     */
    protected ResultSet currentResultSet = null;
    /**
     * The update count.
     */
    protected int updateCount = -1;
    /**
     * Whether the escape processing is on or off. By default, it is on, even Cassandra implementation currently does
     * not take it into account.
     */
    protected boolean escapeProcessing = true;
    /**
     * The Java Driver for Apache Cassandra® statement.
     */
    protected com.datastax.oss.driver.api.core.cql.Statement<?> statement;
    /**
     * The consistency level used for the statement.
     */
    protected ConsistencyLevel consistencyLevel;
    private boolean isClosed;
    private DriverExecutionProfile customTimeoutProfile;

    /**
     * Constructor. It instantiates a new Cassandra statement with default values and a {@code null} CQL statement for
     * a {@link CassandraConnection}.
     * <p>
     * By default, the result set type is {@link ResultSet#TYPE_FORWARD_ONLY}, the result set concurrency is
     * {@link ResultSet#CONCUR_READ_ONLY} and the result set holdability is
     * {@link ResultSet#HOLD_CURSORS_OVER_COMMIT}.
     * </p>
     *
     * @param connection The Cassandra connection to the database.
     * @throws SQLException when something went wrong during the instantiation of the statement.
     */
    CassandraStatement(final CassandraConnection connection) throws SQLException {
        this(connection, null, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Constructor. It instantiates a new Cassandra statement with default values for a {@link CassandraConnection}.
     * <p>
     * By default, the result set type is {@link ResultSet#TYPE_FORWARD_ONLY}, the result set concurrency is
     * {@link ResultSet#CONCUR_READ_ONLY} and the result set holdability is
     * {@link ResultSet#HOLD_CURSORS_OVER_COMMIT}.
     * </p>
     *
     * @param connection The Cassandra connection to the database.
     * @param cql        The CQL statement.
     * @throws SQLException when something went wrong during the instantiation of the statement.
     */
    CassandraStatement(final CassandraConnection connection, final String cql) throws SQLException {
        this(connection, cql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Constructor. It instantiates a new Cassandra statement with default holdability and specified result set type
     * and concurrency for a {@link CassandraConnection}.
     * <p>
     * By default, the result set holdability is {@link ResultSet#HOLD_CURSORS_OVER_COMMIT}.
     * </p>
     *
     * @param connection           The Cassandra connection to the database.
     * @param cql                  The CQL statement.
     * @param resultSetType        The result set type.
     * @param resultSetConcurrency The result set concurrency.
     * @throws SQLException when something went wrong during the instantiation of the statement.
     */
    CassandraStatement(final CassandraConnection connection, final String cql, final int resultSetType,
                       final int resultSetConcurrency) throws SQLException {
        this(connection, cql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Constructor. It instantiates a new Cassandra statement with specified result set type, concurrency and
     * holdability for a {@link CassandraConnection}.
     *
     * @param connection           The Cassandra connection to the database.
     * @param cql                  The CQL statement.
     * @param resultSetType        The result set type.
     * @param resultSetConcurrency The result set concurrency.
     * @param resultSetHoldability The result set holdability.
     * @throws SQLException            when something went wrong during the instantiation of the statement.
     * @throws SQLSyntaxErrorException when an argument for result set configuration is invalid.
     */
    CassandraStatement(final CassandraConnection connection, final String cql, final int resultSetType,
                       final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
        this.connection = connection;
        this.cql = cql;
        this.batchQueries = new ArrayList<>();
        this.consistencyLevel = connection.getConsistencyLevel();
        this.fetchSize = connection.getDefaultFetchSize();
        this.isClosed = false;

        if (!(resultSetType == ResultSet.TYPE_FORWARD_ONLY
            || resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE
            || resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE)) {
            throw new SQLSyntaxErrorException(String.format(BAD_TYPE_RS, resultSetType));
        }
        this.resultSetType = resultSetType;

        if (!(resultSetConcurrency == ResultSet.CONCUR_READ_ONLY
            || resultSetConcurrency == ResultSet.CONCUR_UPDATABLE)) {
            throw new SQLSyntaxErrorException(String.format(BAD_CONCURRENCY_RS, resultSetConcurrency));
        }
        this.resultSetConcurrency = resultSetConcurrency;

        if (!(resultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT
            || resultSetHoldability == ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
            throw new SQLSyntaxErrorException(String.format(BAD_HOLD_RS, resultSetHoldability));
        }
        this.resultSetHoldability = resultSetHoldability;
    }

    @Override
    public void addBatch(final String query) throws SQLException {
        checkNotClosed();
        batchQueries.add(query);
    }

    /**
     * Checks that the statement is not closed.
     *
     * @throws SQLException            when something went wrong during the checking of the statement status.
     * @throws SQLRecoverableException when a method has been called on a closed statement.
     */
    protected final void checkNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLRecoverableException(WAS_CLOSED_STMT);
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        checkNotClosed();
        this.batchQueries = new ArrayList<>();
    }

    @Override
    public void clearWarnings() throws SQLException {
        // This implementation does not support the collection of warnings so clearing is a no-op, but it still throws
        // an exception when called on a closed statement.
        checkNotClosed();
    }

    @Override
    public void close() {
        this.isClosed = true;
        this.cql = null;
    }

    @Override
    public int compareTo(@Nonnull final Object target) {
        if (this.equals(target)) {
            return 0;
        }
        if (this.hashCode() < target.hashCode()) {
            return -1;
        }
        return 1;
    }

    private List<String> splitStatements(final String cql) {
        final String[] cqlQueries = cql.split(STATEMENTS_SEPARATOR_REGEX);
        final List<String> cqlQueriesToExecute = new ArrayList<>(cqlQueries.length);

        // If a query contains a semicolon character in a string value (for example 'abc;xyz'), re-merge queries
        // wrongly split.
        final String singleQuote = "'";
        StringBuilder prevCqlQuery = new StringBuilder();
        for (final String cqlQuery : cqlQueries) {
            final boolean hasStringValues = cqlQuery.contains(singleQuote);
            final boolean isFirstQueryPartWithIncompleteStringValue =
                countMatches(cqlQuery, singleQuote) % 2 == 1 && prevCqlQuery.length() == 0;
            final boolean isNotFirstQueryPartWithCompleteStringValue =
                countMatches(cqlQuery, singleQuote) % 2 == 0 && prevCqlQuery.length() > 0;
            final boolean isNotFirstQueryPartWithoutStringValue =
                !prevCqlQuery.toString().isEmpty() && !cqlQuery.contains(singleQuote);

            if ((hasStringValues && (isFirstQueryPartWithIncompleteStringValue
                || isNotFirstQueryPartWithCompleteStringValue)) || isNotFirstQueryPartWithoutStringValue) {
                prevCqlQuery.append(cqlQuery).append(";");
            } else {
                prevCqlQuery.append(cqlQuery);
                cqlQueriesToExecute.add(prevCqlQuery.toString());
                prevCqlQuery = new StringBuilder();
            }
        }
        return cqlQueriesToExecute;
    }

    private boolean doExecuteCustom(final String cql) throws SQLException {
        for (StatementExecutor executor : EXECUTORS) {
            StatementExecutor.ExecutionResult result = executor.execute(connection, cql);
            if (result != null) {
                this.currentResultSet = result.resultSet;
                return true;
            }
        }
        return false;
    }

    private void doExecute(final String cql) throws SQLException {
        if (doExecuteCustom(cql)) {
            return;
        }

        final List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>();

        try {
            final List<String> cqlQueries = splitStatements(cql);
            final int nbQueriesToExecute = cqlQueries.size();
            if (nbQueriesToExecute > 1
                && !(cql.trim().toLowerCase().startsWith("begin")
                && cql.toLowerCase().contains("batch") && cql.toLowerCase().contains("apply"))) {
                final ArrayList<com.datastax.oss.driver.api.core.cql.ResultSet> results = new ArrayList<>();

                // Several statements in the query to execute potentially asynchronously...
                if (nbQueriesToExecute > MAX_ASYNC_QUERIES * 1.1) {
                    // Protect the cluster from receiving too many queries at once and force the dev to split the load
                    throw new SQLNonTransientException(String.format(TOO_MANY_QUERIES, nbQueriesToExecute));
                }

                // If we should not execute the queries asynchronously, for example if they must be executed in the
                // specified order (e.g. in Liquibase scripts with queries such as CREATE TABLE t, then
                // INSERT INTO t ...).
                if (!this.connection.getOptionSet().executeMultipleQueriesByStatementAsync()) {
                    for (final String cqlQuery : cqlQueries) {
                        final com.datastax.oss.driver.api.core.cql.ResultSet rs = executeSingleStatement(cqlQuery);
                        results.add(rs);
                    }
                } else {
                    for (final String cqlQuery : cqlQueries) {
                        if (LOG.isDebugEnabled() || this.connection.isDebugMode()) {
                            LOG.debug("CQL: {}", cqlQuery);
                        }
                        SimpleStatement stmt = SimpleStatement.newInstance(cqlQuery)
                            .setConsistencyLevel(this.connection.getConsistencyLevel())
                            .setPageSize(this.fetchSize);
                        if (this.customTimeoutProfile != null) {
                            stmt = stmt.setExecutionProfile(this.customTimeoutProfile);
                        }
                        final CompletionStage<AsyncResultSet> resultSetFuture =
                            ((CqlSession) this.connection.getSession()).executeAsync(stmt);
                        futures.add(resultSetFuture);
                    }

                    for (final CompletionStage<AsyncResultSet> future : futures) {
                        final AsyncResultSet asyncResultSet = CompletableFutures.getUninterruptibly(future);
                        final com.datastax.oss.driver.api.core.cql.ResultSet rows;
                        if (asyncResultSet.hasMorePages()) {
                            rows = new MultiPageResultSet(asyncResultSet);
                        } else {
                            rows = new SinglePageResultSet(asyncResultSet);
                        }
                        results.add(rows);
                    }
                }

                this.currentResultSet = new CassandraResultSet(this, results);
            } else {
                // Only one statement to execute, so do it synchronously.
                this.currentResultSet = new CassandraResultSet(this, executeSingleStatement(cql));
            }
        } catch (final Exception e) {
            for (final CompletionStage<AsyncResultSet> future : futures) {
                future.toCompletableFuture().cancel(true);
            }
            throw new SQLTransientException(e);
        }
    }

    private com.datastax.oss.driver.api.core.cql.ResultSet executeSingleStatement(final String cql) {
        if (LOG.isTraceEnabled() || this.connection.isDebugMode()) {
            LOG.debug("CQL: {}", cql);
        }
        SimpleStatement stmt = SimpleStatement.newInstance(cql)
            .setConsistencyLevel(this.connection.getConsistencyLevel())
            .setPageSize(this.fetchSize);
        if (this.customTimeoutProfile != null) {
            stmt = stmt.setExecutionProfile(this.customTimeoutProfile);
        }
        return ((CqlSession) this.connection.getSession()).execute(stmt);
    }

    @Override
    public boolean execute(final String query) throws SQLException {
        checkNotClosed();
        doExecute(query);
        // Return true if the first result is a non-null ResultSet object; false if the first result is an update count
        // or there is no result.
        return this.currentResultSet != null &&
            (!(this.currentResultSet instanceof CassandraResultSet) || ((CassandraResultSet) this.currentResultSet).isQuery());
    }

    @Override
    public boolean execute(final String cql, final int autoGeneratedKeys) throws SQLException {
        checkNotClosed();
        if (!(autoGeneratedKeys == RETURN_GENERATED_KEYS || autoGeneratedKeys == NO_GENERATED_KEYS)) {
            throw new SQLSyntaxErrorException(String.format(BAD_AUTO_GEN, autoGeneratedKeys));
        }
        if (autoGeneratedKeys == RETURN_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException(NO_GEN_KEYS);
        }
        return execute(cql);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        final int[] returnCounts = new int[this.batchQueries.size()];
        final List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>();
        if (LOG.isTraceEnabled() || this.connection.isDebugMode()) {
            LOG.debug("CQL statements: {}", this.batchQueries.size());
        }

        for (final String query : this.batchQueries) {
            if (LOG.isTraceEnabled() || this.connection.isDebugMode()) {
                LOG.debug("CQL: {}", query);
            }
            SimpleStatement stmt = SimpleStatement.newInstance(query)
                .setConsistencyLevel(this.connection.getConsistencyLevel());
            if (this.customTimeoutProfile != null) {
                stmt = stmt.setExecutionProfile(this.customTimeoutProfile);
            }
            final CompletionStage<AsyncResultSet> resultSetFuture =
                ((CqlSession) this.connection.getSession()).executeAsync(stmt);
            futures.add(resultSetFuture);
        }

        int i = 0;
        for (final CompletionStage<AsyncResultSet> future : futures) {
            CompletableFutures.getUninterruptibly(future);
            returnCounts[i] = 1;
            i++;
        }

        return returnCounts;
    }

    @Override
    public ResultSet executeQuery(final String cql) throws SQLException {
        checkNotClosed();
        doExecute(cql);
        if (this.currentResultSet == null) {
            throw new SQLNonTransientException(NO_RESULT_SET);
        }
        return currentResultSet;
    }

    /**
     * Executes the given CQL statement, which may be an {@code INSERT}, {@code UPDATE}, or {@code DELETE} statement or
     * a CQL statement that returns nothing, such as a CQL DDL statement.
     * <p>
     * <b>Note:</b> This method cannot be called on a {@link PreparedStatement} or {@link CallableStatement}.
     * </p>
     *
     * @param cql A CQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or
     *            {@code DELETE}; or a CQL statement that returns nothing, such as a DDL statement.
     * @return Always 0, for any statement. The rationale is that Datastax Java driver does not provide update count.
     * @throws SQLException when something went wrong during the execution of the statement.
     */
    @Override
    public int executeUpdate(final String cql) throws SQLException {
        checkNotClosed();
        doExecute(cql);
        return connection.getOptionSet().getSQLUpdateResponse();
    }

    @Override
    public int executeUpdate(final String cql, final int autoGeneratedKeys) throws SQLException {
        checkNotClosed();
        if (!(autoGeneratedKeys == RETURN_GENERATED_KEYS || autoGeneratedKeys == NO_GENERATED_KEYS)) {
            throw new SQLFeatureNotSupportedException(String.format(BAD_AUTO_GEN, autoGeneratedKeys));
        }
        return executeUpdate(cql);
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkNotClosed();
        return this.connection;
    }

    /**
     * Retrieves the {@link CassandraConnection} object that produced this {@link Statement} object.
     *
     * @return The Cassandra connection that produced this statement.
     * @throws SQLException if a database access error occurs or this method is called on a closed {@link Statement}.
     */
    public CassandraConnection getCassandraConnection() throws SQLException {
        return (CassandraConnection) getConnection();
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return this.consistencyLevel;
    }

    @Override
    public void setConsistencyLevel(final ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        this.statement = this.statement.setConsistencyLevel(consistencyLevel);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkNotClosed();
        return this.fetchDirection;
    }

    @Override
    public void setFetchDirection(final int direction) throws SQLException {
        checkNotClosed();
        if (direction == ResultSet.FETCH_FORWARD || direction == ResultSet.FETCH_REVERSE
            || direction == ResultSet.FETCH_UNKNOWN) {
            if (getResultSetType() == ResultSet.TYPE_FORWARD_ONLY && direction != ResultSet.FETCH_FORWARD) {
                throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
            }
            this.fetchDirection = direction;
        } else {
            throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkNotClosed();
        return this.fetchSize;
    }

    @Override
    public void setFetchSize(final int rows) throws SQLException {
        checkNotClosed();
        if (rows < 0) {
            throw new SQLSyntaxErrorException(String.format(BAD_FETCH_SIZE, rows));
        }
        this.fetchSize = rows;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkNotClosed();
        return this.maxFieldSize;
    }

    /**
     * Sets the limit for the maximum number of bytes that can be returned for character and binary column values in a
     * {@link ResultSet} object produced by this {@link Statement} object.
     * <p>
     * This setting is silently ignored. There is no such limit, so {@link #maxFieldSize} is always 0.
     * </p>
     *
     * @param max The new column size limit in bytes; zero means there is no limit.
     * @throws SQLException if a database access error occurs or this method is called on a closed {@link Statement}.
     */
    @Override
    public void setMaxFieldSize(final int max) throws SQLException {
        checkNotClosed();
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkNotClosed();
        return this.maxRows;
    }

    /**
     * Sets the limit for the maximum number of rows that any {@link ResultSet} object generated by this
     * {@link Statement} object can contain to the given number. If the limit is exceeded, the excess rows are silently
     * dropped.
     * <p>
     * This setting is silently ignored. There is no such limit, so {@link #maxRows} is always 0.
     * </p>
     *
     * @param max The new max rows limit; zero means there is no limit.
     * @throws SQLException if a database access error occurs or this method is called on a closed {@link Statement}.
     */
    @Override
    public void setMaxRows(final int max) throws SQLException {
        checkNotClosed();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkNotClosed();
        resetResults();
        // in the current Cassandra implementation there are never more results
        return false;
    }

    @Override
    public boolean getMoreResults(final int current) throws SQLException {
        checkNotClosed();

        switch (current) {
            case CLOSE_CURRENT_RESULT:
                resetResults();
                break;
            case CLOSE_ALL_RESULTS:
            case KEEP_CURRENT_RESULT:
                throw new SQLFeatureNotSupportedException(NO_MULTIPLE);
            default:
                throw new SQLSyntaxErrorException(String.format(BAD_KEEP_RS, current));
        }
        // In the current Cassandra implementation there are never more results.
        return false;
    }

    /**
     * Retrieves the number of seconds the driver will wait for a {@link Statement} object to execute. If the limit is
     * exceeded, a {@link SQLException} is thrown.
     *
     * @return The current query timeout limit in seconds; zero means there is no limit.
     * @throws SQLException if a database access error occurs or this method is called on a closed {@link Statement}.
     */
    @Override
    public int getQueryTimeout() throws SQLException {
        checkNotClosed();
        DriverExecutionProfile activeProfile =
            this.connection.getSession().getContext().getConfig().getDefaultProfile();
        if (this.customTimeoutProfile != null) {
            activeProfile = this.customTimeoutProfile;
        }
        return Long.valueOf(Objects.requireNonNull(
            activeProfile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ZERO)
        ).get(ChronoUnit.SECONDS)).intValue();
    }

    /**
     * Sets the number of seconds the driver will wait for a {@link Statement} object to execute.
     * <p>
     * Modifying this setting will derive the default execution profile of the driver configuration by updating the
     * parameter {@code basic.request.timeout} and apply this specific execution profile to this statement ONLY. Later
     * statements will use the request timeout globally configured for the session.
     * </p>
     *
     * @param seconds The new query timeout limit in seconds; zero means there is no limit.
     * @throws SQLException if a database access error occurs, this method is called on a closed {@link Statement}.
     */
    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {
        checkNotClosed();
        final DriverExecutionProfile defaultProfile =
            this.connection.getSession().getContext().getConfig().getDefaultProfile();
        this.customTimeoutProfile =
            defaultProfile.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(seconds));
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkNotClosed();
        return this.currentResultSet;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkNotClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkNotClosed();
        // The Cassandra implementation does not support commits, so this is the closest match.
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkNotClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkNotClosed();
        return this.updateCount;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // The rationale is there are no warnings to return in this implementation, but it still throws an exception
        // when called on a closed result set.
        checkNotClosed();
        return null;
    }

    @Override
    public boolean isClosed() {
        return this.isClosed;
    }

    /**
     * Returns a value indicating whether the {@link Statement} is pool-able or not.
     *
     * @return Always {@code false}.
     * @throws SQLException if this method is called on a closed {@link Statement}.
     */
    @Override
    public boolean isPoolable() throws SQLException {
        checkNotClosed();
        return false;
    }

    /**
     * Requests that a {@link Statement} be pooled or not pooled.
     * <p>
     * This setting is silently ignored. The {@code CassandraStatement} are never pool-able.
     * </p>
     *
     * @param poolable Requests that the statement be pooled if {@code true} and that the statement not be pooled if
     *                 {@code false}.
     * @throws SQLException if this method is called on a closed {@link Statement}.
     */
    @Override
    public void setPoolable(final boolean poolable) throws SQLException {
        checkNotClosed();
    }

    /**
     * Resets the current result set for this statement.
     */
    protected final void resetResults() {
        this.currentResultSet = null;
        this.updateCount = -1;
    }

    @Override
    public void setEscapeProcessing(final boolean enable) throws SQLException {
        checkNotClosed();
        // The Cassandra implementation does not currently take this into account.
        this.escapeProcessing = enable;
    }

}
