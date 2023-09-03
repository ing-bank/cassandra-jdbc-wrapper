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

import java.sql.SQLException;

/**
 * Option set implementation for Liquibase compatibility and flavour of JDBC.
 */
public class Liquibase extends AbstractOptionSet {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOptionSet.class);

    @Override
    public String getCatalog() {
        if (getConnection() == null) {
            return null;
        }
        try {
            return getConnection().getSchema();
        } catch (final SQLException e) {
            LOG.warn("Unable to retrieve the schema name: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public int getSQLUpdateResponse() {
        return -1;
    }

    @Override
    public boolean shouldThrowExceptionOnRollback() {
        return false;
    }

    @Override
    public boolean executeMultipleQueriesByStatementAsync() {
        return false;
    }
}
