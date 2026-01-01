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

import java.sql.Array;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.WAS_CLOSED_CONN;

/**
 * Cassandra connection from a pool managed by a {@link PooledCassandraDataSource}.
 */
class ManagedConnection extends AbstractConnection implements Connection {

    private PooledCassandraConnection pooledCassandraConnection;
    private CassandraConnection physicalConnection;
    private final Set<Statement> statements = new HashSet<>();

    /**
     * Constructor.
     *
     * @param pooledCassandraConnection The underlying {@link PooledCassandraConnection}.
     */
    ManagedConnection(final PooledCassandraConnection pooledCassandraConnection) {
        this.pooledCassandraConnection = pooledCassandraConnection;
        this.physicalConnection = pooledCassandraConnection.getConnection();
    }

    private void checkNotClosed() throws SQLNonTransientConnectionException {
        if (isClosed()) {
            throw new SQLNonTransientConnectionException(WAS_CLOSED_CONN);
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkNotClosed();
        try {
            this.physicalConnection.clearWarnings();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public synchronized void close() throws SQLException {
        for (final Statement statement : this.statements) {
            if (!statement.isClosed()) {
                statement.close();
            }
        }
        this.pooledCassandraConnection.connectionClosed();
        this.pooledCassandraConnection = null;
        this.physicalConnection = null;
    }

    @Override
    public void commit() throws SQLException {
        checkNotClosed();
        try {
            this.physicalConnection.commit();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public Array createArrayOf(final String typeName, final Object[] objects) throws SQLException {
        checkNotClosed();
        return this.physicalConnection.createArrayOf(typeName, objects);
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkNotClosed();
        return this.physicalConnection.createBlob();
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkNotClosed();
        final Statement statement = this.physicalConnection.createStatement();
        this.statements.add(statement);
        return statement;
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
        checkNotClosed();
        final Statement statement = this.physicalConnection.createStatement(resultSetType, resultSetConcurrency);
        this.statements.add(statement);
        return statement;
    }

    @Override
    public Statement createStatement(final int resultSetType,
                                     final int resultSetConcurrency,
                                     final int resultSetHoldability) throws SQLException {
        checkNotClosed();
        final Statement statement =
            this.physicalConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        this.statements.add(statement);
        return statement;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkNotClosed();
        try {
            return this.physicalConnection.getAutoCommit();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        checkNotClosed();
        try {
            this.physicalConnection.setAutoCommit(autoCommit);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        checkNotClosed();
        try {
            return this.physicalConnection.getCatalog();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setCatalog(final String catalog) throws SQLException {
        checkNotClosed();
        try {
            this.physicalConnection.setCatalog(catalog);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkNotClosed();
        try {
            return physicalConnection.getClientInfo();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public String getClientInfo(final String name) throws SQLException {
        checkNotClosed();
        try {
            return this.physicalConnection.getClientInfo(name);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setClientInfo(final Properties properties) {
        if (!isClosed()) {
            this.physicalConnection.setClientInfo(properties);
        }
    }

    @Override
    public void setClientInfo(final String name, final String value) {
        if (!isClosed()) {
            this.physicalConnection.setClientInfo(name, value);
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        checkNotClosed();
        try {
            return this.physicalConnection.getHoldability();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setHoldability(final int holdability) throws SQLException {
        checkNotClosed();
        try {
            this.physicalConnection.setHoldability(holdability);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkNotClosed();
        try {
            return this.physicalConnection.getMetaData();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkNotClosed();
        try {
            return this.physicalConnection.isReadOnly();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setReadOnly(final boolean readOnly) throws SQLException {
        checkNotClosed();
        try {
            this.physicalConnection.setReadOnly(readOnly);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public String getSchema() throws SQLException {
        checkNotClosed();
        try {
            return this.physicalConnection.getSchema();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setSchema(final String schema) throws SQLException {
        checkNotClosed();
        try {
            this.physicalConnection.setSchema(schema);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkNotClosed();
        try {
            return this.physicalConnection.getTransactionIsolation();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setTransactionIsolation(final int level) throws SQLException {
        checkNotClosed();
        try {
            this.physicalConnection.setTransactionIsolation(level);
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkNotClosed();
        try {
            return this.physicalConnection.getTypeMap();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkNotClosed();
        try {
            return this.physicalConnection.getWarnings();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean isClosed() {
        return this.physicalConnection == null;
    }

    @Override
    public boolean isValid(final int timeout) throws SQLTimeoutException {
        return this.physicalConnection.isValid(timeout);
    }

    @Override
    public String nativeSQL(final String sql) throws SQLException {
        checkNotClosed();
        try {
            return physicalConnection.nativeSQL(sql);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public PreparedStatement prepareStatement(final String cql) throws SQLException {
        checkNotClosed();
        final ManagedPreparedStatement statement = this.pooledCassandraConnection.prepareStatement(this, cql);
        this.statements.add(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(final String sql,
                                              final int resultSetType,
                                              final int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public PreparedStatement prepareStatement(final String sql,
                                              final int resultSetType,
                                              final int resultSetConcurrency,
                                              final int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void rollback() throws SQLException {
        checkNotClosed();
        try {
            this.physicalConnection.rollback();
        } catch (final SQLException sqlException) {
            this.pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

}
