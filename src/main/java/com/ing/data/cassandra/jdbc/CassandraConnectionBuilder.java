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

import com.ing.data.cassandra.jdbc.utils.ContactPoint;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.ShardingKey;
import java.util.List;

import static com.ing.data.cassandra.jdbc.utils.WarningConstants.UNSUPPORTED_SHARDING_KEYS;

/**
 * Cassandra connection builder: implementation class for {@link ConnectionBuilder}.
 */
@Slf4j
public class CassandraConnectionBuilder implements ConnectionBuilder {

    private final CassandraDataSource dataSource;

    CassandraConnectionBuilder(final CassandraDataSource dataSource) {
        this.dataSource = dataSource;
    }

    private ConnectionBuilder runThenReturnBuilder(final Runnable operation) {
        operation.run();
        return this;
    }

    @Override
    public ConnectionBuilder user(final String username) {
        return runThenReturnBuilder(() -> this.dataSource.setUser(username));
    }

    @Override
    public ConnectionBuilder password(final String password) {
        return runThenReturnBuilder(() -> this.dataSource.setPassword(password));
    }

    /**
     * Specifies the contact points to be used when creating a connection.
     *
     * @param contactPoints The contact points to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public ConnectionBuilder contactPoints(final List<ContactPoint> contactPoints) {
        return runThenReturnBuilder(() -> this.dataSource.setContactPoints(contactPoints));
    }

    // TODO: implement other properties.

    @Override
    public ConnectionBuilder shardingKey(final ShardingKey shardingKey) {
        // Sharding keys are not supported, this is a no-op method.
        return runThenReturnBuilder(() -> log.warn(UNSUPPORTED_SHARDING_KEYS));
    }

    @Override
    public ConnectionBuilder superShardingKey(final ShardingKey shardingKey) {
        // Super sharding keys are not supported, this is a no-op method.
        return runThenReturnBuilder(() -> log.warn(UNSUPPORTED_SHARDING_KEYS));
    }

    @Override
    public Connection build() throws SQLException {
        return this.dataSource.getConnection();
    }

}
