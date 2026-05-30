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

import javax.sql.XAConnection;
import javax.sql.XAConnectionBuilder;
import javax.sql.XADataSource;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.ShardingKeyBuilder;
import java.util.logging.Logger;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;

/**
 * Cassandra data source for distributed transactions: implementation class for {@link XADataSource}.
 * <p><b>IMPORTANT:</b> distributed transactions are currently not supported.</p>
 */
public class CassandraXADataSource implements XADataSource {

    @Override
    public XAConnection getXAConnection() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public XAConnection getXAConnection(final String user, final String password) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public PrintWriter getLogWriter() {
        return DriverManager.getLogWriter();
    }

    @Override
    public void setLogWriter(final PrintWriter out) {
        DriverManager.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(final int timeout) {
        DriverManager.setLoginTimeout(timeout);
    }

    @Override
    public int getLoginTimeout() {
        return DriverManager.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public ShardingKeyBuilder createShardingKeyBuilder() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public XAConnectionBuilder createXAConnectionBuilder() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

}
