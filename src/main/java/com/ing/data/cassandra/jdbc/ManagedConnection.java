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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
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

import static com.ing.data.cassandra.jdbc.Utils.NOT_SUPPORTED;

class ManagedConnection extends AbstractConnection implements Connection {
    private PooledCassandraConnection pooledCassandraConnection;

    private CassandraConnection physicalConnection;

    private final Set<Statement> statements = new HashSet<Statement>();

    ManagedConnection(final PooledCassandraConnection pooledCassandraConnection) {
        this.pooledCassandraConnection = pooledCassandraConnection;
        this.physicalConnection = pooledCassandraConnection.getConnection();
    }

    private void checkNotClosed() throws SQLNonTransientConnectionException {
        if (isClosed()) {
            throw new SQLNonTransientConnectionException(Utils.WAS_CLOSED_CON);
        }
    }

    @Override
    public boolean isClosed() {
        return physicalConnection == null;
    }

    @Override
    public synchronized void close() throws SQLException {
        for (final Statement statement : statements) {
            if (!statement.isClosed()) {
                statement.close();
            }
        }
        pooledCassandraConnection.connectionClosed();
        pooledCassandraConnection = null;
        physicalConnection = null;
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException(String.format(Utils.NO_INTERFACE, iface.getSimpleName()));
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkNotClosed();
        final Statement statement = physicalConnection.createStatement();
        statements.add(statement);
        return statement;
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
        checkNotClosed();
        final Statement statement = physicalConnection.createStatement(resultSetType, resultSetConcurrency);
        statements.add(statement);
        return statement;
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency,
                                     final int resultSetHoldability) throws SQLException {
        checkNotClosed();
        final Statement statement = physicalConnection.createStatement(resultSetType, resultSetConcurrency,
            resultSetHoldability);
        statements.add(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(final String cql) throws SQLException {
        checkNotClosed();
        final ManagedPreparedStatement statement = pooledCassandraConnection.prepareStatement(this, cql);
        statements.add(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency,
                                              final int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public boolean isValid(final int timeout) throws SQLTimeoutException {
        return physicalConnection.isValid(timeout);
    }

    @Override
    public void setClientInfo(final String name, final String value) throws SQLClientInfoException {
        if (!isClosed()) {
            physicalConnection.setClientInfo(name, value);
        }
    }

    @Override
    public void setClientInfo(final Properties properties) throws SQLClientInfoException {
        if (!isClosed()) {
            physicalConnection.setClientInfo(properties);
        }
    }

    // All the following methods use the pattern checkNotClosed() ; try-return/void ; catch-notify-throw

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
    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        checkNotClosed();
        try {
            physicalConnection.setAutoCommit(autoCommit);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkNotClosed();
        try {
            return physicalConnection.getAutoCommit();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void commit() throws SQLException {
        checkNotClosed();
        try {
            physicalConnection.commit();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void rollback() throws SQLException {
        checkNotClosed();
        try {
            physicalConnection.rollback();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkNotClosed();
        try {
            return physicalConnection.getMetaData();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setReadOnly(final boolean readOnly) throws SQLException {
        checkNotClosed();
        try {
            physicalConnection.setReadOnly(readOnly);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkNotClosed();
        try {
            return physicalConnection.isReadOnly();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setCatalog(final String catalog) throws SQLException {
        checkNotClosed();
        try {
            physicalConnection.setCatalog(catalog);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        checkNotClosed();
        try {
            return physicalConnection.getCatalog();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setTransactionIsolation(final int level) throws SQLException {
        checkNotClosed();
        try {
            physicalConnection.setTransactionIsolation(level);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkNotClosed();
        try {
            return physicalConnection.getTransactionIsolation();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkNotClosed();
        try {
            return physicalConnection.getWarnings();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkNotClosed();
        try {
            physicalConnection.clearWarnings();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public void setHoldability(final int holdability) throws SQLException {
        checkNotClosed();
        try {
            physicalConnection.setHoldability(holdability);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        checkNotClosed();
        try {
            return physicalConnection.getHoldability();
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
            throw sqlException;
        }
    }

    @Override
    public String getClientInfo(final String name) throws SQLException {
        checkNotClosed();
        try {
            return physicalConnection.getClientInfo(name);
        } catch (final SQLException sqlException) {
            pooledCassandraConnection.connectionErrorOccurred(sqlException);
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
    public String getSchema() {
        // TODO
        return null;
    }

    @Override
    public void setSchema(final String arg0) {
        // TODO
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        // TODO
        return null;
    }
}
