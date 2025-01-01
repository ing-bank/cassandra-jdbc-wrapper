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

import com.ing.data.cassandra.jdbc.UsingCassandraContainerTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class CopyToCommandTest extends UsingCassandraContainerTest {

    private static final Logger LOG = LoggerFactory.getLogger(CopyToCommandTest.class);
    private static final String KEYSPACE = "test_keyspace";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
    }

    @Test
    void givenTableAndTargetFile_whenExecuteCopyToCommand_generateExpectedCsvFile() throws SQLException {
        assertNotNull(sqlConnection);
        // TODO sqlConnection.createStatement().execute("COPY cf_test1(keyname, t1ivalue) TO 'test_result.csv'");
    }

}
