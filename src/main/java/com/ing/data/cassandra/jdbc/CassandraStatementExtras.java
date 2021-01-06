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

import java.sql.Statement;

/**
 * Extension of {@link Statement} interface providing additional methods specific to Cassandra statements.
 */
public interface CassandraStatementExtras extends Statement {

    /**
     * Sets the consistency level for the statement.
     * <p>
     *     See <a href="https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html">
     *     consistency level documentation</a> for further details.
     * </p>
     *
     * @param consistencyLevel The consistency level to use for this statement.
     */
    void setConsistencyLevel(ConsistencyLevel consistencyLevel);

    /**
     * Gets the consistency level for the statement.
     * <p>
     *     See <a href="https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html">
     *     consistency level documentation</a> for further details.
     * </p>
     *
     * @return The consistency level used for this statement.
     */
    ConsistencyLevel getConsistencyLevel();

}
