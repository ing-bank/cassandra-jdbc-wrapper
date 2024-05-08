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

import static com.ing.data.cassandra.jdbc.utils.ConsistencyLevelUtils.assertConsistencyLevel;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DefaultConsistencyLevelTest extends UsingCassandraContainerTest {
    private static final String KEYSPACE = "test_keyspace";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1", "consistency=SERIAL");
    }

    @Test
    void testDefaultConsistencyLevel() throws Exception {
        assertNotNull(sqlConnection);
        assertConsistencyLevel(sqlConnection, "SERIAL");
    }
}
