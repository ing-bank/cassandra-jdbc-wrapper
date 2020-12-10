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

import com.google.common.io.CharStreams;

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

import static com.ing.data.cassandra.jdbc.Utils.WAS_CLOSED_CON;

class ManagedPreparedStatement extends AbstractStatement implements PreparedStatement {
    private final PooledCassandraConnection pooledCassandraConnection;

    private final ManagedConnection managedConnection;

    private CassandraPreparedStatement preparedStatement;

    private boolean poolable;

    ManagedPreparedStatement(final PooledCassandraConnection pooledCassandraConnection,
                             final ManagedConnection managedConnection,
                             final CassandraPreparedStatement preparedStatement) {
        this.pooledCassandraConnection = pooledCassandraConnection;
        this.managedConnection = managedConnection;
        this.preparedStatement = preparedStatement;
    }

    private void checkNotClosed() throws SQLNonTransientConnectionException {
        if (isClosed()) {
            throw new SQLNonTransientConnectionException(WAS_CLOSED_CON);
        }
    }

    @Override
    public ManagedConnection getConnection() {
        return managedConnection;
    }

    @Override
    public boolean isClosed() {
        return preparedStatement == null || preparedStatement.isClosed();
    }

    @Override
    public void close() throws SQLNonTransientConnectionException {
        checkNotClosed();
        pooledCassandraConnection.statementClosed(preparedStatement);
        preparedStatement = null;
    }

    @Override
    public boolean isPoolable() {
        return poolable;
    }

    @Override
    public void setPoolable(final boolean poolable) {
        this.poolable = poolable;
    }

    @Override
    public int hashCode() {
        return preparedStatement.getCql().hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    // All the following methods use the pattern checkNotClosed() ; try-return/void ; catch-notify-throw

    @Override
    public void addBatch(final String arg0) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.addBatch(arg0);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.clearBatch();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.clearWarnings();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean execute(final String arg0) throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.execute(arg0);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean execute(final String arg0, final int arg1) throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.execute(arg0, arg1);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.executeBatch();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public ResultSet executeQuery(final String arg0) throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.executeQuery(arg0);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int executeUpdate(final String arg0) throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.executeUpdate(arg0);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int executeUpdate(final String arg0, final int arg1) throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.executeUpdate(arg0, arg1);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getFetchDirection();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getFetchSize();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getMaxFieldSize();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getMaxRows();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getMoreResults();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean getMoreResults(final int arg0) throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getMoreResults(arg0);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getQueryTimeout();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getResultSet();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getResultSetConcurrency();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getResultSetHoldability();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getResultSetType();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getUpdateCount();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getWarnings();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setEscapeProcessing(final boolean arg0) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setEscapeProcessing(arg0);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setFetchDirection(final int arg0) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setFetchDirection(arg0);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setFetchSize(final int arg0) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setFetchSize(arg0);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setMaxFieldSize(final int arg0) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setMaxFieldSize(arg0);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setMaxRows(final int arg0) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setMaxRows(arg0);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setQueryTimeout(final int arg0) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setQueryTimeout(arg0);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean isWrapperFor(final Class<?> arg0) throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.isWrapperFor(arg0);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public <T> T unwrap(final Class<T> arg0) throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.unwrap(arg0);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void addBatch() throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.addBatch();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.clearParameters();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean execute() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.execute();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.executeQuery();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.executeUpdate();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getMetaData();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkNotClosed();
        try {
            return preparedStatement.getParameterMetaData();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setBigDecimal(final int parameterIndex, final BigDecimal x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setBigDecimal(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setBoolean(final int parameterIndex, final boolean x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setBoolean(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setByte(final int parameterIndex, final byte x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setByte(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setBytes(final int parameterIndex, final byte[] x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setBytes(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setDate(final int parameterIndex, final Date x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setDate(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setDate(final int parameterIndex, final Date x, final Calendar cal) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setDate(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setDouble(final int parameterIndex, final double x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setDouble(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setFloat(final int parameterIndex, final float x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setFloat(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setInt(final int parameterIndex, final int x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setInt(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setLong(final int parameterIndex, final long x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setLong(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setNString(final int parameterIndex, final String value) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setNString(parameterIndex, value);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setNull(parameterIndex, sqlType);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType, final String typeName) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setNull(parameterIndex, sqlType, typeName);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setObject(final int parameterIndex, final Object x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setObject(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setObject(parameterIndex, x, targetSqlType);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType,
                          final int scaleOrLength) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setRowId(final int parameterIndex, final RowId x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setRowId(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setShort(final int parameterIndex, final short x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setShort(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setString(final int parameterIndex, final String x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setString(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setTime(final int parameterIndex, final Time x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setTime(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setTime(final int parameterIndex, final Time x, final Calendar cal) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setTime(parameterIndex, x, cal);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setTimestamp(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setTimestamp(parameterIndex, x, cal);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setURL(final int parameterIndex, final URL x) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setURL(parameterIndex, x);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setBlob(final int parameterIndex, final Blob value) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setBlob(parameterIndex, value);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setBlob(final int parameterIndex, final InputStream value) throws SQLException {
        checkNotClosed();
        try {
            preparedStatement.setBlob(parameterIndex, value);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader, final int length)
        throws SQLException {
        try {
            final String targetString = CharStreams.toString(reader);
            reader.close();

            setString(parameterIndex, targetString);
        } catch (final IOException e) {
            throw new SQLNonTransientException(e);
        }
    }
}
