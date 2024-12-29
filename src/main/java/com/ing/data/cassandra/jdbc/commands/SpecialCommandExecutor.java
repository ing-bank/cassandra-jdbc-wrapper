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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.ing.data.cassandra.jdbc.CassandraStatement;

import java.sql.SQLException;

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
    ResultSet execute(CassandraStatement statement, String cql) throws SQLException;

}
