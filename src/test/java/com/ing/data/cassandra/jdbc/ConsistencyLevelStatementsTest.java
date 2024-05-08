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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Statement;

import static com.ing.data.cassandra.jdbc.utils.ConsistencyLevelUtils.assertConsistencyLevel;
import static com.ing.data.cassandra.jdbc.utils.ConsistencyLevelUtils.assertConsistencyLevelViaExecute;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConsistencyLevelStatementsTest extends UsingCassandraContainerTest {
    private static final String KEYSPACE = "test_keyspace";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
    }

    @Test
    void testSetConsistencyLevel() throws Exception {
        assertNotNull(sqlConnection);
        Statement statement = sqlConnection.createStatement();
        boolean isQuery = statement.execute("CONSISTENCY EACH_QUORUM");
        assertFalse(isQuery);
        assertConsistencyLevel(sqlConnection, "EACH_QUORUM");
    }

    @Test
    void testSetConsistencyLevelViaExecute() throws Exception {
        assertNotNull(sqlConnection);
        sqlConnection.createStatement().execute("CONSISTENCY LOCAL_SERIAL");
        assertConsistencyLevelViaExecute(sqlConnection, "LOCAL_SERIAL");
    }
}
