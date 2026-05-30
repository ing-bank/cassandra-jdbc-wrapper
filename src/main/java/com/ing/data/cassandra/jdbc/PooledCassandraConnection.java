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

import lombok.extern.slf4j.Slf4j;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Pooled Cassandra connection: implementation class for {@link PooledConnection} to create a JDBC pooled connection to
 * a Cassandra cluster.
 */
@Slf4j
public class PooledCassandraConnection implements PooledConnection {

    volatile Set<ConnectionEventListener> connectionEventListeners = new HashSet<>();
    volatile Set<StatementEventListener> statementEventListeners = new HashSet<>();

    private final CassandraConnection physicalConnection;
    private final Map<String, Set<CassandraPreparedStatement>> freePreparedStatements = new HashMap<>();
    private final Map<String, Set<CassandraPreparedStatement>> usedPreparedStatements = new HashMap<>();

    /**
     * Constructor.
     *
     * @param physicalConnection The physical {@link CassandraConnection}.
     */
    public PooledCassandraConnection(final CassandraConnection physicalConnection) {
        this.physicalConnection = physicalConnection;
    }

    @Override
    public CassandraConnection getConnection() {
        return this.physicalConnection;
    }

    @Override
    public void close() throws SQLException {
        this.physicalConnection.close();
    }

    @Override
    public void addConnectionEventListener(final ConnectionEventListener listener) {
        this.connectionEventListeners.add(listener);
    }

    @Override
    public void removeConnectionEventListener(final ConnectionEventListener listener) {
        this.connectionEventListeners.remove(listener);
    }

    @Override
    public void addStatementEventListener(final StatementEventListener listener) {
        this.statementEventListeners.add(listener);
    }

    @Override
    public void removeStatementEventListener(final StatementEventListener listener) {
        this.statementEventListeners.remove(listener);
    }

    /**
     * Notifies each registered {@link ConnectionEventListener} that the application has called the method
     * close() on its representation of the pooled connection (here a {@link ManagedConnection}).
     *
     * @see ManagedConnection#close()
     */
    void connectionClosed() {
        final ConnectionEvent event = new ConnectionEvent(this);
        this.connectionEventListeners.forEach(listener -> listener.connectionClosed(event));
    }

    /**
     * Notifies each registered {@link ConnectionEventListener} that a fatal error has occurred and the pooled
     * connection can no longer be used.
     *
     * @param sqlException The SQL exception.
     * @see ManagedConnection
     */
    void connectionErrorOccurred(final SQLException sqlException) {
        final ConnectionEvent event = new ConnectionEvent(this, sqlException);
        this.connectionEventListeners.forEach(listener -> listener.connectionErrorOccurred(event));
    }

    /**
     * Notifies each registered {@link StatementEventListener} that a prepared statement is closed.
     *
     * @param preparedStatement The prepared statement.
     * @see ManagedPreparedStatement#close()
     */
    void statementClosed(final CassandraPreparedStatement preparedStatement) {
        final StatementEvent event = new StatementEvent(this, preparedStatement);
        this.statementEventListeners.forEach(listener -> listener.statementClosed(event));

        final String cql = preparedStatement.getCql();
        final Set<CassandraPreparedStatement> freeStatements = this.freePreparedStatements.get(cql);
        final Set<CassandraPreparedStatement> usedStatements = this.usedPreparedStatements.get(cql);

        usedStatements.remove(preparedStatement);

        preparedStatement.resetResults();
        try {
            preparedStatement.clearParameters();
            freeStatements.add(preparedStatement);
        } catch (final SQLException e) {
            log.error(e.getMessage());
        }
    }

    /**
     * Notifies each registered {@link StatementEventListener} that a prepared statement is invalid.
     *
     * @param preparedStatement The prepared statement.
     * @param sqlException      The SQL exception.
     * @see ManagedPreparedStatement
     */
    void statementErrorOccurred(final CassandraPreparedStatement preparedStatement, final SQLException sqlException) {
        final StatementEvent event = new StatementEvent(this, preparedStatement, sqlException);
        this.statementEventListeners.forEach(listener -> listener.statementErrorOccurred(event));

        final String cql = preparedStatement.getCql();
        final Set<CassandraPreparedStatement> usedStatements = this.usedPreparedStatements.get(cql);

        if (!(event.getSQLException() instanceof SQLRecoverableException)) {
            preparedStatement.close();
            usedStatements.remove(preparedStatement);
        }
    }

    /**
     * Creates a {@link PreparedStatement} object for sending parameterized SQL statements to the database on a
     * {@link ManagedConnection} representation of the pooled connection.
     *
     * @param managedConnection The representation of the pooled connection.
     * @param cql               The CQL statement.
     * @return The instantiated {@link ManagedPreparedStatement}.
     * @throws SQLException when something went wrong during the creation of the prepared statement.
     */
    public synchronized ManagedPreparedStatement prepareStatement(final ManagedConnection managedConnection,
                                                                  final String cql) throws SQLException {
        this.freePreparedStatements.putIfAbsent(cql, new HashSet<>());
        this.usedPreparedStatements.putIfAbsent(cql, new HashSet<>());

        final Set<CassandraPreparedStatement> freeStatements = this.freePreparedStatements.get(cql);
        final Set<CassandraPreparedStatement> usedStatements = this.usedPreparedStatements.get(cql);

        final CassandraPreparedStatement managedPreparedStatement;
        if (freeStatements.isEmpty()) {
            managedPreparedStatement = this.physicalConnection.prepareStatement(cql);
        } else {
            managedPreparedStatement = freeStatements.iterator().next();
            freeStatements.remove(managedPreparedStatement);
        }
        usedStatements.add(managedPreparedStatement);

        return new ManagedPreparedStatement(this, managedConnection, managedPreparedStatement);
    }

}
