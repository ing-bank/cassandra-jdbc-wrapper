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

package com.ing.data.cassandra.jdbc.optionset;

import com.ing.data.cassandra.jdbc.CassandraConnection;

/**
 * Option set for compliance mode.
 * Different use cases require one or more adjustments to the wrapper, to be compatible.
 * Thus, {@code OptionSet} would provide convenience to set for different flavours (for example Liquibase expect some
 * methods return values different of the JDBC standard implementation).
 */
public interface OptionSet {
    /**
      * There is no catalog concept in Cassandra. Different flavour requires different response.
     *
      * @return The current catalog name or {@code null} if there is none.
     */
    String getCatalog();

    /**
     * There is no {@code updateCount} available in Datastax Java driver, different flavour requires different response.
     *
     * @return A predefined update response.
     */
    int getSQLUpdateResponse();

    /**
     * Set referenced connection. See @{@link AbstractOptionSet}.
     * @param connection Connection to set.
     */
    void setConnection(CassandraConnection connection);

    /**
     * Get referenced connection. See @{@link AbstractOptionSet}.
     *
     * @return the referenced connection.
     */
    CassandraConnection getConnection();
}
