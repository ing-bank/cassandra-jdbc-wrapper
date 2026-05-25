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
import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.ShardingKey;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NON_NULL_DATASOURCE_REQUIRED;

/**
 * Cassandra connection builder: implementation class for {@link ConnectionBuilder}.
 */
public class CassandraConnectionBuilder extends AbstractCassandraConnectionBuilder<Connection>
    implements ConnectionBuilder {

    CassandraConnectionBuilder(final CassandraDataSource dataSource) throws SQLException {
        if (dataSource == null) {
            throw new SQLException(NON_NULL_DATASOURCE_REQUIRED);
        }
        this.setDataSource(dataSource);
    }

    @Override
    public CassandraConnectionBuilder user(final String username) {
        return (CassandraConnectionBuilder) super.user(username);
    }

    @Override
    public CassandraConnectionBuilder password(final String password) {
        return (CassandraConnectionBuilder) super.password(password);
    }

    @Override
    public CassandraConnectionBuilder shardingKey(final ShardingKey shardingKey) {
        return (CassandraConnectionBuilder) super.shardingKey(shardingKey);
    }

    @Override
    public CassandraConnectionBuilder superShardingKey(final ShardingKey shardingKey) {
        return (CassandraConnectionBuilder) super.superShardingKey(shardingKey);
    }

    @Override
    public Connection build() throws SQLException {
        return this.getDataSource().getConnection();
    }

}
