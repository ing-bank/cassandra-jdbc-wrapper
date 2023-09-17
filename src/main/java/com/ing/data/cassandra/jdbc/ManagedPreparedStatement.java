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

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.WAS_CLOSED_CONN;

/**
 * Cassandra prepared statement executed in the context of a pool of connections managed in a
 * {@link PooledCassandraConnection}.
 */
class ManagedPreparedStatement extends AbstractStatement implements PreparedStatement {

    private final PooledCassandraConnection pooledCassandraConnection;
    private final ManagedConnection managedConnection;
    private CassandraPreparedStatement preparedStatement;
    private boolean poolable;

    /**
     * Constructor.
     *
     * @param pooledCassandraConnection The pooled connection to Cassandra database.
     * @param managedConnection         The managed connection.
     * @param preparedStatement         The prepared statement.
     */
    ManagedPreparedStatement(final PooledCassandraConnection pooledCassandraConnection,
                             final ManagedConnection managedConnection,
                             final CassandraPreparedStatement preparedStatement) {
        this.pooledCassandraConnection = pooledCassandraConnection;
        this.managedConnection = managedConnection;
        this.preparedStatement = preparedStatement;
    }

    private void checkNotClosed() throws SQLNonTransientConnectionException {
        if (isClosed()) {
            throw new SQLNonTransientConnectionException(WAS_CLOSED_CONN);
        }
    }

    @Override
    public ManagedConnection getConnection() {
        return this.managedConnection;
    }

    @Override
    public void close() throws SQLNonTransientConnectionException {
        checkNotClosed();
        this.pooledCassandraConnection.statementClosed(this.preparedStatement);
        this.preparedStatement = null;
    }

    @Override
    public boolean isClosed() {
        return this.preparedStatement == null || this.preparedStatement.isClosed();
    }

    @Override
    public boolean isPoolable() {
        return this.poolable;
    }

    @Override
    public void setPoolable(final boolean poolable) {
        this.poolable = poolable;
    }

    @Override
    public int hashCode() {
        return this.preparedStatement.getCql().hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    @Override
    public void addBatch(final String cql) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.addBatch(cql);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void addBatch() throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.addBatch();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.clearBatch();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.clearWarnings();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean execute() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.execute();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean execute(final String cql) throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.execute(cql);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean execute(final String cql, final int autoGeneratedKeys) throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.execute(cql, autoGeneratedKeys);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.executeBatch();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.executeQuery();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public ResultSet executeQuery(final String cql) throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.executeQuery(cql);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.executeUpdate();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int executeUpdate(final String cql) throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.executeUpdate(cql);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int executeUpdate(final String cql, final int autoGeneratedKeys) throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.executeUpdate(cql, autoGeneratedKeys);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getFetchDirection();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setFetchDirection(final int direction) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setFetchDirection(direction);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getFetchSize();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setFetchSize(final int rows) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setFetchSize(rows);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getMaxFieldSize();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setMaxFieldSize(final int max) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setMaxFieldSize(max);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getMaxRows();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setMaxRows(final int max) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setMaxRows(max);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getMoreResults();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean getMoreResults(final int current) throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getMoreResults(current);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getQueryTimeout();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setQueryTimeout(seconds);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getResultSet();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getResultSetConcurrency();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getResultSetHoldability();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getResultSetType();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getUpdateCount();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getWarnings();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setEscapeProcessing(final boolean enable) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setEscapeProcessing(enable);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.isWrapperFor(iface);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.unwrap(iface);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.clearParameters();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getMetaData();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkNotClosed();
        try {
            return this.preparedStatement.getParameterMetaData();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setBigDecimal(final int parameterIndex, final BigDecimal x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setBigDecimal(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setBlob(final int parameterIndex, final Blob value) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setBlob(parameterIndex, value);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setBlob(final int parameterIndex, final InputStream value) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setBlob(parameterIndex, value);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setBoolean(final int parameterIndex, final boolean x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setBoolean(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setByte(final int parameterIndex, final byte x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setByte(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setBytes(final int parameterIndex, final byte[] x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setBytes(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader, final int length)
        throws SQLException {
        try {
            final String targetString = IOUtils.toString(reader);
            reader.close();
            setString(parameterIndex, targetString);
        } catch (final IOException e) {
            throw new SQLNonTransientException(e);
        }
    }

    @Override
    public void setDate(final int parameterIndex, final Date x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setDate(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setDate(final int parameterIndex, final Date x, final Calendar cal) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setDate(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setDouble(final int parameterIndex, final double x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setDouble(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setFloat(final int parameterIndex, final float x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setFloat(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setInt(final int parameterIndex, final int x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setInt(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setLong(final int parameterIndex, final long x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setLong(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setNString(final int parameterIndex, final String value) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setNString(parameterIndex, value);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setNull(parameterIndex, sqlType);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType, final String typeName) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setNull(parameterIndex, sqlType, typeName);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setObject(final int parameterIndex, final Object x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setObject(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setObject(parameterIndex, x, targetSqlType);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType,
                          final int scaleOrLength) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setRowId(final int parameterIndex, final RowId x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setRowId(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setShort(final int parameterIndex, final short x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setShort(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setString(final int parameterIndex, final String x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setString(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setTime(final int parameterIndex, final Time x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setTime(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setTime(final int parameterIndex, final Time x, final Calendar cal) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setTime(parameterIndex, x, cal);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setTimestamp(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setTimestamp(parameterIndex, x, cal);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setURL(final int parameterIndex, final URL x) throws SQLException {
        checkNotClosed();
        try {
            this.preparedStatement.setURL(parameterIndex, x);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.statementErrorOccurred(this.preparedStatement, sqlException);
            throw sqlException;
        }
    }

}
