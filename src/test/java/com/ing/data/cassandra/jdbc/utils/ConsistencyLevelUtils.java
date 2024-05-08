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

package com.ing.data.cassandra.jdbc.utils;

import com.ing.data.cassandra.jdbc.CassandraConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsistencyLevelUtils {
    public static void assertConsistencyLevel(CassandraConnection sqlConnection, String level) throws SQLException {
        ResultSet resultSet = sqlConnection.createStatement().executeQuery("CONSISTENCY");
        assertEquals(1, resultSet.findColumn("consistency_level"));
        assertTrue(resultSet.next());
        assertEquals(level, resultSet.getString(1));
    }

    public static void assertConsistencyLevelViaExecute(CassandraConnection sqlConnection, String level) throws SQLException {
        Statement statement = sqlConnection.createStatement();
        boolean isQuery = statement.execute("CONSISTENCY");
        assertTrue(isQuery);
        ResultSet resultSet = statement.getResultSet();
        assertEquals(1, resultSet.findColumn("consistency_level"));
        assertTrue(resultSet.next());
        assertEquals(level, resultSet.getString(1));
    }
}
