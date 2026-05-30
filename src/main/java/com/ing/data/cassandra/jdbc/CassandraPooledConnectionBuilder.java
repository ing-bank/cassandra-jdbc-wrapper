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

import javax.sql.PooledConnection;
import javax.sql.PooledConnectionBuilder;
import java.sql.SQLException;
import java.sql.ShardingKey;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NON_NULL_DATASOURCE_REQUIRED;

/**
 * Cassandra pooled connection builder: implementation class for {@link PooledConnectionBuilder}.
 */
public class CassandraPooledConnectionBuilder extends AbstractCassandraConnectionBuilder<PooledConnection>
    implements PooledConnectionBuilder {

    CassandraPooledConnectionBuilder(final CassandraDataSource dataSource) throws SQLException {
        if (dataSource == null) {
            throw new SQLException(NON_NULL_DATASOURCE_REQUIRED);
        }
        this.setDataSource(dataSource);
    }

    @Override
    public CassandraPooledConnectionBuilder user(final String username) {
        return (CassandraPooledConnectionBuilder) super.user(username);
    }

    @Override
    public CassandraPooledConnectionBuilder password(final String password) {
        return (CassandraPooledConnectionBuilder) super.password(password);
    }

    @Override
    public CassandraPooledConnectionBuilder shardingKey(final ShardingKey shardingKey) {
        return (CassandraPooledConnectionBuilder) super.shardingKey(shardingKey);
    }

    @Override
    public CassandraPooledConnectionBuilder superShardingKey(final ShardingKey shardingKey) {
        return (CassandraPooledConnectionBuilder) super.superShardingKey(shardingKey);
    }

    @Override
    public PooledConnection build() throws SQLException {
        return this.getDataSource().getPooledConnection();
    }

}
