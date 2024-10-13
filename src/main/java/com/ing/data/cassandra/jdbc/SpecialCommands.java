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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.ing.data.cassandra.jdbc.utils.SpecialCommandsUtil;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Locale;

import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.ing.data.cassandra.jdbc.utils.ByteBufferUtil.bytes;
import static com.ing.data.cassandra.jdbc.utils.SpecialCommandsUtil.buildEmptyResultSet;
import static com.ing.data.cassandra.jdbc.utils.SpecialCommandsUtil.buildSpecialCommandResultSet;

/**
 * Handler for <a href="https://cassandra.apache.org/doc/stable/cassandra/tools/cqlsh.html#special-commands">special
 * CQL commands</a>.
 * <p>
 *     Supported commands:
 *     <ul>
 *         <li>{@code CONSISTENCY [level]}</li>
 *         <li>{@code SERIAL CONSISTENCY [level]}</li>
 *         </ul>
 * </p>
 */
public final class SpecialCommands {

    /**
     * Executor for a
     * <a href="https://cassandra.apache.org/doc/stable/cassandra/tools/cqlsh.html#special-commands">special CQL
     * command</a>.
     */
    public interface SpecialCommandExecutor {
        /**
         * Executes the given CQL statement as a special command not handled by the Java driver.
         *
         * @param statement The Cassandra statement.
         * @param cql       The CQL statement to execute.
         * @return The result set corresponding to the CQL special command or an empty result set (see
         * {@link SpecialCommandsUtil#buildEmptyResultSet()}) if the command is not expected to return some results.
         * @throws SQLException if something went wrong with the Cassandra statement.
         */
        com.datastax.oss.driver.api.core.cql.ResultSet execute(CassandraStatement statement,
                                                               String cql) throws SQLException;
    }

    /**
     * Executor returning an empty result set.
     */
    public static class NoOpExecutor implements SpecialCommandExecutor {
        @Override
        public ResultSet execute(final CassandraStatement statement, final String cql) throws SQLException {
            return buildEmptyResultSet();
        }
    }

    /**
     * Executor for consistency level special commands.
     * <p>
     *     {@code CONSISTENCY [level]}: if {@code level} is specified, set the current consistency level of the
     *     connection to the given value, otherwise return a return a result set with a single row containing the
     *     current consistency level in a column {@code consistency_level}.
     * </p>
     */
    public static class ConsistencyLevelExecutor implements SpecialCommandExecutor {

        private String levelParameter = null;

        /**
         * Constructor.
         *
         * @param levelParameter The parameter {@code level} of the command, if defined.
         */
        public ConsistencyLevelExecutor(@Nullable final String levelParameter) {
            if (levelParameter != null) {
                this.levelParameter = levelParameter.toUpperCase(Locale.ENGLISH);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public com.datastax.oss.driver.api.core.cql.ResultSet execute(final CassandraStatement statement,
                                                                      final String cql) throws SQLException {
            final CassandraConnection connection = (CassandraConnection) statement.getConnection();

            if (this.levelParameter != null) {
                final ConsistencyLevel newConsistencyLevel = DefaultConsistencyLevel.valueOf(this.levelParameter);
                connection.setConsistencyLevel(newConsistencyLevel);
                // Also update the consistency level of the current statement (in case this one contains several
                // statements to execute, especially after the consistency level update).
                statement.setConsistencyLevel(newConsistencyLevel);
                return buildEmptyResultSet();
            } else {
                final String currentLevel = connection.getConsistencyLevel().name();

                // Create a result set with a single row containing the current consistency level value in a column
                // 'consistency_level'.
                final ByteBuffer currentLevelAsBytes = bytes(currentLevel);
                return buildSpecialCommandResultSet(
                    new ColumnDefinitions.Definition[]{
                        ColumnDefinitions.Definition.buildDefinitionInAnonymousTable("consistency_level", TEXT)
                    },
                    Collections.singletonList(
                        Collections.singletonList(currentLevelAsBytes)
                    )
                );
            }
        }
    }

    /**
     * Executor for serial consistency level special commands.
     * <p>
     *     {@code SERIAL CONSISTENCY [level]}: if {@code level} is specified, set the current serial consistency level
     *     of the connection to the given value, otherwise return a return a result set with a single row containing the
     *     current serial consistency level in a column {@code serial_consistency_level}.
     * </p>
     */
    public static class SerialConsistencyLevelExecutor implements SpecialCommandExecutor {

        private String levelParameter = null;

        /**
         * Constructor.
         *
         * @param levelParameter The parameter {@code level} of the command, if defined.
         */
        public SerialConsistencyLevelExecutor(@Nullable final String levelParameter) {
            if (levelParameter != null) {
                this.levelParameter = levelParameter.toUpperCase(Locale.ENGLISH);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public com.datastax.oss.driver.api.core.cql.ResultSet execute(final CassandraStatement statement,
                                                                      final String cql) throws SQLException {
            final CassandraConnection connection = (CassandraConnection) statement.getConnection();

            if (this.levelParameter != null) {
                final ConsistencyLevel newConsistencyLevel = DefaultConsistencyLevel.valueOf(this.levelParameter);
                connection.setSerialConsistencyLevel(newConsistencyLevel);
                // Also update the serial consistency level of the current statement (in case this one contains several
                // statements to execute, especially after the serial consistency level update).
                statement.setSerialConsistencyLevel(newConsistencyLevel);
                return buildEmptyResultSet();
            } else {
                final String currentLevel = connection.getSerialConsistencyLevel().name();

                // Create a result set with a single row containing the current consistency level value in a column
                // 'serial_consistency_level'.
                final ByteBuffer currentLevelAsBytes = bytes(currentLevel);
                return buildSpecialCommandResultSet(
                    new ColumnDefinitions.Definition[]{
                        ColumnDefinitions.Definition.buildDefinitionInAnonymousTable("serial_consistency_level", TEXT)
                    },
                    Collections.singletonList(
                        Collections.singletonList(currentLevelAsBytes)
                    )
                );
            }
        }
    }
}
