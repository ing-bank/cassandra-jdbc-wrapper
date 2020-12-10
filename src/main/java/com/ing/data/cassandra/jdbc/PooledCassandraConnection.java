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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class PooledCassandraConnection implements PooledConnection {
    private static final Logger log = LoggerFactory.getLogger(PooledCassandraConnection.class);

    private final CassandraConnection physicalConnection;
    volatile Set<ConnectionEventListener> connectionEventListeners = new HashSet<>();
    volatile Set<StatementEventListener> statementEventListeners = new HashSet<>();
    private final Map<String, Set<CassandraPreparedStatement>> freePreparedStatements = new HashMap<>();
    private final Map<String, Set<CassandraPreparedStatement>> usedPreparedStatements = new HashMap<>();

    public PooledCassandraConnection(final CassandraConnection physicalConnection) {
        this.physicalConnection = physicalConnection;
    }

    @Override
    public CassandraConnection getConnection() {
        return physicalConnection;
    }

    @Override
    public void close() throws SQLException {
        physicalConnection.close();
    }

    @Override
    public void addConnectionEventListener(final ConnectionEventListener listener) {
        connectionEventListeners.add(listener);
    }

    @Override
    public void removeConnectionEventListener(final ConnectionEventListener listener) {
        connectionEventListeners.remove(listener);
    }

    @Override
    public void addStatementEventListener(final StatementEventListener listener) {
        statementEventListeners.add(listener);
    }

    @Override
    public void removeStatementEventListener(final StatementEventListener listener) {
        statementEventListeners.remove(listener);
    }

    void connectionClosed() {
        final ConnectionEvent event = new ConnectionEvent(this);
        for (final ConnectionEventListener listener : connectionEventListeners) {
            listener.connectionClosed(event);
        }
    }

    void connectionErrorOccurred(final SQLException sqlException) {
        final ConnectionEvent event = new ConnectionEvent(this, sqlException);
        for (final ConnectionEventListener listener : connectionEventListeners) {
            listener.connectionErrorOccurred(event);
        }
    }

    void statementClosed(final CassandraPreparedStatement preparedStatement) {
        final StatementEvent event = new StatementEvent(this, preparedStatement);
        for (final StatementEventListener listener : statementEventListeners) {
            listener.statementClosed(event);
        }

        final String cql = preparedStatement.getCql();
        final Set<CassandraPreparedStatement> freeStatements = freePreparedStatements.get(cql);
        final Set<CassandraPreparedStatement> usedStatements = usedPreparedStatements.get(cql);

        usedStatements.remove(preparedStatement);

        preparedStatement.resetResults();
        try {
            preparedStatement.clearParameters();
            freeStatements.add(preparedStatement);
        } catch (final SQLException e) {
            log.error(e.getMessage());
        }
    }

    void statementErrorOccurred(final CassandraPreparedStatement preparedStatement, final SQLException sqlException) {
        final StatementEvent event = new StatementEvent(this, preparedStatement, sqlException);
        for (final StatementEventListener listener : statementEventListeners) {
            listener.statementErrorOccurred(event);
        }

        final String cql = preparedStatement.getCql();
        final Set<CassandraPreparedStatement> usedStatements = usedPreparedStatements.get(cql);

        if (!(event.getSQLException() instanceof SQLRecoverableException)) {
            preparedStatement.close();
            usedStatements.remove(preparedStatement);
        }
    }

    public synchronized ManagedPreparedStatement prepareStatement(final ManagedConnection managedConnection,
                                                                  final String cql) throws SQLException {
        if (!freePreparedStatements.containsKey(cql)) {
            freePreparedStatements.put(cql, new HashSet<>());
            usedPreparedStatements.put(cql, new HashSet<>());
        }

        final Set<CassandraPreparedStatement> freeStatements = freePreparedStatements.get(cql);
        final Set<CassandraPreparedStatement> usedStatements = usedPreparedStatements.get(cql);

        final CassandraPreparedStatement managedPreparedStatement;
        if (freeStatements.isEmpty()) {
            managedPreparedStatement = physicalConnection.prepareStatement(cql);
        } else {
            managedPreparedStatement = freeStatements.iterator().next();
            freeStatements.remove(managedPreparedStatement);
        }
        usedStatements.add(managedPreparedStatement);

        return new ManagedPreparedStatement(this, managedConnection, managedPreparedStatement);
    }
}
