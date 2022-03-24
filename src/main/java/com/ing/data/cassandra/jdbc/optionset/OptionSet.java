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
 * Option Set for compliance mode.
 * Different use cases require one or more adjustments to the wrapper, to be compatible.
 * Thus, OptionSet would provide convenience to set for different flavours.
 *
 */
public interface OptionSet {
    /**
      * There is no Catalog concept in cassandra. Different flavour requires different response.
     *
      * @return Catalog
     */
    String getCatalog();

    /**
     * There is no updateCount available in Datastax Java driver, different flavour requires different response.
     *
     * @return Predefined update response
     */
    int getSQLUpdateResponse();

    /**
     * Set referenced connection. See @{@link AbstractOptionSet}
     * @param connection Connection to set
     */
    void setConnection(CassandraConnection connection);

    /**
     * Get referenced connection. See @{@link AbstractOptionSet}
     *
     * @return referenced connection
     */
    CassandraConnection getConnection();
}
