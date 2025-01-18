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

import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class CopyFromCommandTest extends UsingCassandraContainerTest {

    private static final Logger LOG = LoggerFactory.getLogger(CopyFromCommandTest.class);
    private static final String KEYSPACE = "test_keyspace";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
    }

    static String getTestOriginPath(final String csvFile) {
        final URL cqlScriptResourceUrl = CopyFromCommandTest.class.getClassLoader().getResource(csvFile);
        if (cqlScriptResourceUrl == null) {
            fail("Could not find the CSV script to import: " + csvFile);
        }
        return cqlScriptResourceUrl.getPath();
    }

    @Test
    void givenTableAndOriginFile_whenExecuteCopyFromCommand_executeExpectedStatements() throws SQLException {
        assertNotNull(sqlConnection);
        sqlConnection.createStatement()
            .execute(String.format("COPY cf_test1 FROM '%s'", getTestOriginPath("test_copy_from_cmd.csv")));

        // Verify the rows to insert as specified in the executed script are effectively present.
        final PreparedStatement verifyStmt = sqlConnection.prepareStatement(
            "SELECT keyname, t1bValue, t1iValue FROM cf_test1 WHERE keyname = ?");
        verifyStmt.setString(1, "key200");
        ResultSet verifyRs = verifyStmt.executeQuery();
        assertTrue(verifyRs.next());
        assertEquals("key200", verifyRs.getString("keyname"));
        assertTrue(verifyRs.getBoolean("t1bValue"));
        assertEquals(654, verifyRs.getInt("t1iValue"));
    }

}
