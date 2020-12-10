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
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.internal.core.cql.MultiPageResultSet;
import com.datastax.oss.driver.internal.core.cql.SinglePageResultSet;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Cassandra statement: implementation class for {@link PreparedStatement}.
 */
public class CassandraStatement extends AbstractStatement
    implements CassandraStatementExtras, Comparable<Object>, Statement {
    public static final int MAX_ASYNC_QUERIES = 1000;
    public static final String SEMI_COLON_REGEX = ";";
    private static final Logger log = LoggerFactory.getLogger(CassandraStatement.class);

    /**
     * The connection.
     */
    protected CassandraConnection connection;

    /**
     * The CQL statement.
     */
    protected String cql;
    protected ArrayList<String> batchQueries;

    protected int fetchDirection = ResultSet.FETCH_FORWARD;

    protected int fetchSize = 100;

    protected int maxFieldSize = 0;

    protected int maxRows = 0;

    protected int resultSetType = CassandraResultSet.DEFAULT_TYPE;

    protected int resultSetConcurrency = CassandraResultSet.DEFAULT_CONCURRENCY;

    protected int resultSetHoldability = CassandraResultSet.DEFAULT_HOLDABILITY;

    protected ResultSet currentResultSet = null;

    protected int updateCount = -1;

    protected boolean escapeProcessing = true;

    protected com.datastax.oss.driver.api.core.cql.Statement statement;

    protected ConsistencyLevel consistencyLevel;

    CassandraStatement(final CassandraConnection con) throws SQLException {
        this(con, null, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    CassandraStatement(final CassandraConnection con, final String cql) throws SQLException {
        this(con, cql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    CassandraStatement(final CassandraConnection con, final String cql, final int resultSetType,
                       final int resultSetConcurrency) throws SQLException {
        this(con, cql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    CassandraStatement(final CassandraConnection con, final String cql, final int resultSetType,
                       final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
        this.connection = con;
        this.cql = cql;
        this.batchQueries = Lists.newArrayList();

        this.consistencyLevel = con.defaultConsistencyLevel;

        if (!(resultSetType == ResultSet.TYPE_FORWARD_ONLY
            || resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE
            || resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE)) {
            throw new SQLSyntaxErrorException(Utils.BAD_TYPE_RSET);
        }
        this.resultSetType = resultSetType;

        if (!(resultSetConcurrency == ResultSet.CONCUR_READ_ONLY
            || resultSetConcurrency == ResultSet.CONCUR_UPDATABLE)) {
            throw new SQLSyntaxErrorException(Utils.BAD_TYPE_RSET);
        }
        this.resultSetConcurrency = resultSetConcurrency;

        if (!(resultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT
            || resultSetHoldability == ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
            throw new SQLSyntaxErrorException(Utils.BAD_HOLD_RSET);
        }
        this.resultSetHoldability = resultSetHoldability;
    }

    @Override
    public void addBatch(final String query) throws SQLException {
        checkNotClosed();
        batchQueries.add(query);
    }

    protected final void checkNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLRecoverableException(Utils.WAS_CLOSED_STMT);
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        checkNotClosed();
        batchQueries = new ArrayList<>();
    }

    @Override
    public void clearWarnings() throws SQLException {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    @Override
    public void close() {
        connection = null;
        cql = null;
    }

    private void doExecute(final String cql) throws SQLException {
        final List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>();
        try {
            final String[] cqlQueries = cql.split(SEMI_COLON_REGEX);
            if (cqlQueries.length > 1 && !(cql.trim().toLowerCase().startsWith("begin")
                && cql.toLowerCase().contains("batch") && cql.toLowerCase().contains("apply"))) {
                // several statements in the query to execute asynchronously

                final ArrayList<com.datastax.oss.driver.api.core.cql.ResultSet> results = Lists.newArrayList();
                if (cqlQueries.length > MAX_ASYNC_QUERIES * 1.1) {
                    // Protect the cluster from receiving too many queries at once and force the dev to split the load
                    throw new SQLNonTransientException("Too many queries at once (" + cqlQueries.length
                        + "). You must split your queries into more batches !");
                }
                StringBuilder prevCqlQuery = new StringBuilder();
                for (final String cqlQuery : cqlQueries) {
                    if ((cqlQuery.contains("'") && ((StringUtils.countMatches(cqlQuery, "'") % 2 == 1
                        && prevCqlQuery.length() == 0)
                        || (StringUtils.countMatches(cqlQuery, "'") % 2 == 0 && prevCqlQuery.length() > 0)))
                        || (prevCqlQuery.toString().length() > 0 && !cqlQuery.contains("'"))) {
                        prevCqlQuery.append(cqlQuery + ";");
                    } else {
                        prevCqlQuery.append(cqlQuery);
                        if (log.isTraceEnabled() || this.connection.debugMode) {
                            log.debug("CQL: " + prevCqlQuery.toString());
                        }
                        final SimpleStatement stmt = SimpleStatement.newInstance(prevCqlQuery.toString())
                            .setConsistencyLevel(this.connection.defaultConsistencyLevel)
                            .setPageSize(this.fetchSize);
                        final CompletionStage<AsyncResultSet> resultSetFuture =
                            ((CqlSession) this.connection.getSession()).executeAsync(stmt);
                        futures.add(resultSetFuture);
                        prevCqlQuery = new StringBuilder();
                    }
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

                currentResultSet = new CassandraResultSet(this, results);
            } else {
                // Only one statement to execute so we go synchronous
                if (log.isTraceEnabled() || this.connection.debugMode) {
                    log.debug("CQL: " + cql);
                }
                final SimpleStatement stmt = SimpleStatement.newInstance(cql)
                    .setConsistencyLevel(this.connection.defaultConsistencyLevel)
                    .setPageSize(this.fetchSize);
                currentResultSet = new CassandraResultSet(this,
                    ((CqlSession) this.connection.getSession()).execute(stmt));
            }
        } catch (final Exception e) {
            for (final CompletionStage<AsyncResultSet> future : futures) {
                future.toCompletableFuture().cancel(true);
            }
            throw new SQLTransientException(e);
        }

    }

    @Override
    public boolean execute(final String query) throws SQLException {
        checkNotClosed();
        doExecute(query);
        return !(currentResultSet == null);
    }

    @Override
    public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException {
        checkNotClosed();

        if (!(autoGeneratedKeys == RETURN_GENERATED_KEYS || autoGeneratedKeys == NO_GENERATED_KEYS)) {
            throw new SQLSyntaxErrorException(Utils.BAD_AUTO_GEN);
        }

        if (autoGeneratedKeys == RETURN_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException(Utils.NO_GEN_KEYS);
        }

        return execute(sql);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        final int[] returnCounts = new int[batchQueries.size()];
        final List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>();
        if (log.isTraceEnabled() || this.connection.debugMode) {
            log.debug("CQL statements: " + batchQueries.size());
        }
        for (final String q : batchQueries) {
            if (log.isTraceEnabled() || this.connection.debugMode) {
                log.debug("CQL: " + q);
            }
            final SimpleStatement stmt = SimpleStatement.newInstance(q)
                .setConsistencyLevel(this.connection.defaultConsistencyLevel);
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
    public ResultSet executeQuery(final String query) throws SQLException {
        checkNotClosed();
        doExecute(query);
        if (currentResultSet == null)
            throw new SQLNonTransientException(Utils.NO_RESULTSET);
        return currentResultSet;
    }

    @Override
    public int executeUpdate(final String query) throws SQLException {
        checkNotClosed();
        doExecute(query);
        // no updateCount available in Datastax Java Driver
        return 0;
    }

    @Override
    public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {
        checkNotClosed();

        if (!(autoGeneratedKeys == RETURN_GENERATED_KEYS || autoGeneratedKeys == NO_GENERATED_KEYS)) {
            throw new SQLFeatureNotSupportedException(Utils.BAD_AUTO_GEN);
        }

        return executeUpdate(sql);
    }

    @Override
    @SuppressWarnings("cast")
    public Connection getConnection() throws SQLException {
        checkNotClosed();
        return connection;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkNotClosed();
        return fetchDirection;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkNotClosed();
        return fetchSize;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkNotClosed();
        return maxFieldSize;
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkNotClosed();
        return maxRows;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkNotClosed();
        resetResults();
        // in the current Cassandra implementation there are never more results
        return false;
    }

    @Override
    @SuppressWarnings("boxing")
    public boolean getMoreResults(final int current) throws SQLException {
        checkNotClosed();

        switch (current) {
            case CLOSE_CURRENT_RESULT:
                resetResults();
                break;
            case CLOSE_ALL_RESULTS:
            case KEEP_CURRENT_RESULT:
                throw new SQLFeatureNotSupportedException(Utils.NO_MULTIPLE);
            default:
                throw new SQLSyntaxErrorException(String.format(Utils.BAD_KEEP_RSET, current));
        }
        // in the current Cassandra implementation there are never MORE results
        return false;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        // the Cassandra implementation does not support timeouts on queries
        return 0;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkNotClosed();
        return currentResultSet;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkNotClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkNotClosed();
        // the Cassandra implementations does not support commits so this is the closest match
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
        return updateCount;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkNotClosed();
        return null;
    }

    @Override
    public boolean isClosed() {
        return connection == null;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkNotClosed();
        return false;
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return false;
    }

    protected final void resetResults() {
        currentResultSet = null;
        updateCount = -1;
    }

    @Override
    public void setEscapeProcessing(final boolean enable) throws SQLException {
        checkNotClosed();
        // the Cassandra implementation does not currently look at this
        escapeProcessing = enable;
    }

    @Override
    @SuppressWarnings("boxing")
    public void setFetchDirection(final int direction) throws SQLException {
        checkNotClosed();

        if (direction == ResultSet.FETCH_FORWARD || direction == ResultSet.FETCH_REVERSE
            || direction == ResultSet.FETCH_UNKNOWN) {
            if ((getResultSetType() == ResultSet.TYPE_FORWARD_ONLY) && (direction != ResultSet.FETCH_FORWARD)) {
                throw new SQLSyntaxErrorException(String.format(Utils.BAD_FETCH_DIR, direction));
            }
            fetchDirection = direction;
        } else {
            throw new SQLSyntaxErrorException(String.format(Utils.BAD_FETCH_DIR, direction));
        }
    }

    @Override
    @SuppressWarnings("boxing")
    public void setFetchSize(final int size) throws SQLException {
        checkNotClosed();
        if (size < 0) throw new SQLSyntaxErrorException(String.format(Utils.BAD_FETCH_SIZE, size));
        fetchSize = size;
    }

    @Override
    public void setMaxFieldSize(final int arg0) throws SQLException {
        checkNotClosed();
        // silently ignore this setting. always use default 0 (unlimited)
    }

    public void setMaxRows(int arg0) throws SQLException {
        checkNotClosed();
        // silently ignore this setting. always use default 0 (unlimited)
    }

    @Override
    public void setPoolable(final boolean poolable) throws SQLException {
        checkNotClosed();
        // silently ignore any attempt to set this away from the current default (false)
    }

    @Override
    public void setQueryTimeout(final int arg0) throws SQLException {
        checkNotClosed();
        // silently ignore any attempt to set this away from the current default (0)
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLFeatureNotSupportedException(String.format(Utils.NO_INTERFACE, iface.getSimpleName()));
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    @Override
    public void setConsistencyLevel(final ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        this.statement = this.statement.setConsistencyLevel(consistencyLevel);
    }

    @Override
    public int compareTo(final Object target) {
        if (this.equals(target)) {
            return 0;
        }
        if (this.hashCode() < target.hashCode()) {
            return -1;
        }
        return 1;
    }
}
