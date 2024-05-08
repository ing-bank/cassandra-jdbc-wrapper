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

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

public interface StatementExecutor {
    List<StatementExecutor> EXECUTORS = Arrays.asList(SetConsistencyLevelExecutor.INSTANCE, GetConsistencyLevelExecutor.INSTANCE);


    ExecutionResult execute(final CassandraConnection connection, final String cql) throws SQLException;

    class ExecutionResult {
        final ResultSet resultSet;

        public ExecutionResult(ResultSet resultSet) {
            this.resultSet = resultSet;
        }
    }

    class SetConsistencyLevelExecutor implements StatementExecutor {
        public static final SetConsistencyLevelExecutor INSTANCE = new SetConsistencyLevelExecutor();
        private static final Pattern PATTERN = Pattern.compile("CONSISTENCY (\\w+)", CASE_INSENSITIVE);

        @Override
        public ExecutionResult execute(final CassandraConnection connection, final String cql) throws SQLException {
            Matcher matcher = PATTERN.matcher(cql.trim());
            if (!matcher.matches()) {
                return null;
            }
            String level = matcher.group(1);
            try {
                connection.setConsistencyLevel(DefaultConsistencyLevel.valueOf(level.toUpperCase(Locale.ENGLISH)));
            } catch (IllegalArgumentException e) {
                throw new SQLException(e);
            }
            return new ExecutionResult(null);
        }
    }

    class GetConsistencyLevelExecutor implements StatementExecutor {
        public static final GetConsistencyLevelExecutor INSTANCE = new GetConsistencyLevelExecutor();
        private static final Pattern PATTERN = Pattern.compile("CONSISTENCY", CASE_INSENSITIVE);

        @Override
        public ExecutionResult execute(final CassandraConnection connection, final String cql) throws SQLException {
            Matcher matcher = PATTERN.matcher(cql.trim());
            return matcher.matches()
                ? new ExecutionResult(new ListResultSet(connection.getConsistencyLevel().name(),
                    new ListResultSet.ColumnMetaData("consistency_level", "TEXT", Types.VARCHAR, "java.lang.String")))
                : null;
        }
    }
}
