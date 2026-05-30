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

package com.ing.data.cassandra.jdbc.xa;

import lombok.extern.slf4j.Slf4j;

import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.utils.WarningConstants.NO_OP_CONNECTION_EVENT_LISTENER;
import static com.ing.data.cassandra.jdbc.utils.WarningConstants.NO_OP_STATEMENT_EVENT_LISTENER;

/**
 * Cassandra connection for distributed transactions: implementation class for {@link XAConnection} to create a JDBC
 * connection to a Cassandra cluster for distributed transactions.
 * <p><b>IMPORTANT:</b> distributed transactions are currently not supported.</p>
 */
@Slf4j
public class CassandraXAConnection implements XAConnection {

    @Override
    public XAResource getXAResource() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void close() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void addConnectionEventListener(final ConnectionEventListener connectionEventListener) {
        // XAConnection is not supported by this driver, so this method is not implemented.
        log.warn(NO_OP_CONNECTION_EVENT_LISTENER, "adding");
    }

    @Override
    public void removeConnectionEventListener(final ConnectionEventListener connectionEventListener) {
        // XAConnection is not supported by this driver, so this method is not implemented.
        log.warn(NO_OP_CONNECTION_EVENT_LISTENER, "removing");
    }

    @Override
    public void addStatementEventListener(final StatementEventListener statementEventListener) {
        // XAConnection is not supported by this driver, so this method is not implemented.
        log.warn(NO_OP_STATEMENT_EVENT_LISTENER, "adding");
    }

    @Override
    public void removeStatementEventListener(final StatementEventListener statementEventListener) {
        // XAConnection is not supported by this driver, so this method is not implemented.
        log.warn(NO_OP_STATEMENT_EVENT_LISTENER, "removing");
    }
}
