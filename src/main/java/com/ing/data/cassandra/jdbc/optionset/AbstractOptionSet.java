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
 * Abstract option set to set common parameter used by option sets.
 */
public abstract class AbstractOptionSet implements OptionSet {

    private CassandraConnection connection;

    @Override
    public CassandraConnection getConnection() {
        return connection;
    }

    public void setConnection(final CassandraConnection connection) {
        this.connection = connection;
    }
}
