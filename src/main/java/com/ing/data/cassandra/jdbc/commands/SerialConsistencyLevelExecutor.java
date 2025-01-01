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

package com.ing.data.cassandra.jdbc.commands;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.ing.data.cassandra.jdbc.CassandraConnection;
import com.ing.data.cassandra.jdbc.CassandraStatement;
import com.ing.data.cassandra.jdbc.ColumnDefinitions;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Locale;

import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.buildEmptyResultSet;
import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.buildSpecialCommandResultSet;
import static com.ing.data.cassandra.jdbc.utils.ByteBufferUtil.bytes;

/**
 * Executor for serial consistency level special commands.
 * <p>
 *     {@code SERIAL CONSISTENCY [level]}: if {@code level} is specified, set the current serial consistency level
 *     of the connection to the given value, otherwise return a result set with a single row containing the
 *     current serial consistency level in a column {@code serial_consistency_level}.
 * </p>
 * <p>
 *     The documentation of the original {@code SERIAL CONSISTENCY} command is available:
 *     <ul>
 *         <li><a href="https://cassandra.apache.org/doc/latest/cassandra/managing/tools/cqlsh.html#serial-consistency">
 *             in the Apache Cassandra® documentation</a></li>
 *         <li><a href="https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlshSerialConsistency.html">
 *             in the DataStax CQL reference documentation</a></li>
 *     </ul>
 * </p>
 */
public class SerialConsistencyLevelExecutor implements SpecialCommandExecutor {

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
    public ResultSet execute(final CassandraStatement statement, final String cql) throws SQLException {
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