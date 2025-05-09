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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.ing.data.cassandra.jdbc.utils.WarningConstants.CLUSTER_NAME_NOT_FOUND;

/**
 * Default option set implementing the methods of {@code OptionSet} accordingly to the JDBC API standard.
 */
public class Default extends AbstractOptionSet {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOptionSet.class);

    @Override
    public String getCatalog() {
        // It requires a query to table system.local since Java Driver for Apache Cassandra® 4+.
        // If the query fails, return null.
        try (final Statement stmt = getConnection().createStatement()) {
            final ResultSet rs = stmt.executeQuery("SELECT cluster_name FROM system.local");
            if (rs.next()) {
                return rs.getString("cluster_name");
            }
        } catch (final SQLException e) {
            LOG.warn(CLUSTER_NAME_NOT_FOUND, e.getMessage());
            return null;
        }

        return null;
    }

    @Override
    public int getSQLUpdateResponse() {
        return 0;
    }

    @Override
    public boolean shouldThrowExceptionOnRollback() {
        return true;
    }

    @Override
    public boolean executeMultipleQueriesByStatementAsync() {
        return true;
    }
}
