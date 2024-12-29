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
import com.ing.data.cassandra.jdbc.optionset.Default;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientException;
import java.sql.Statement;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.MISSING_SOURCE_FILENAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SourceCommandTest extends UsingCassandraContainerTest {

    private static final Logger LOG = LoggerFactory.getLogger(SourceCommandTest.class);
    private static final String KEYSPACE = "test_keyspace";
    private static final String USE_FULL_RESOURCE_PATH = "$$USE_FULL_RESOURCE_PATH$$";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
    }

    @Test
    void givenNonExistentFile_whenExecuteSourceCommand_throwException() {
        assertNotNull(sqlConnection);
        final SQLException sqlException = assertThrows(SQLException.class,
            () -> sqlConnection.createStatement().execute("SOURCE 'unknown_file.cql'"));
        assertThat(sqlException.getMessage(), containsString("Could not open 'unknown_file.cql':"));
    }

    @Test
    void givenMissingFilename_whenExecuteSourceCommand_throwException() {
        assertNotNull(sqlConnection);
        final SQLTransientException sqlException = assertThrows(SQLTransientException.class,
            () -> sqlConnection.createStatement().execute("SOURCE "));
        assertThat(sqlException.getCause(), instanceOf(SQLSyntaxErrorException.class));
        assertThat(sqlException.getMessage(), containsString(MISSING_SOURCE_FILENAME));
    }

    @Test
    void givenEmptyFilename_whenExecuteSourceCommand_throwException() {
        assertNotNull(sqlConnection);
        SQLTransientException sqlException = assertThrows(SQLTransientException.class,
            () -> sqlConnection.createStatement().execute("SOURCE ''"));
        assertThat(sqlException.getCause(), instanceOf(SQLSyntaxErrorException.class));
        assertThat(sqlException.getMessage(), containsString(MISSING_SOURCE_FILENAME));

        sqlException = assertThrows(SQLTransientException.class,
            () -> sqlConnection.createStatement().execute("SOURCE '  '"));
        assertThat(sqlException.getCause(), instanceOf(SQLSyntaxErrorException.class));
        assertThat(sqlException.getMessage(), containsString(MISSING_SOURCE_FILENAME));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "src/test/resources/test_source_cmd.cql",
        "~/test_source_cmd.cql",
        USE_FULL_RESOURCE_PATH
    })
    void givenValidCqlFile_whenExecuteSourceCommand_returnExpectedResult(String filename)
        throws SQLException, IOException {
        final URL cqlScriptResourceUrl = this.getClass().getClassLoader().getResource("test_source_cmd.cql");
        if (cqlScriptResourceUrl == null) {
            fail("Could not find the CQL script to execute");
        }

        // For this test, consider the home directory of the user is the temporary directory and copy the CQL script
        // to execute into it.
        if (filename.startsWith("~")) {
            final String tmpDir = System.getProperty("java.io.tmpdir");
            System.setProperty("user.home", tmpDir);
            IOUtils.copy(cqlScriptResourceUrl, new File(tmpDir + File.separator + "test_source_cmd.cql"));
        }

        // Test case where the filename is an absolute path.
        if (USE_FULL_RESOURCE_PATH.equals(filename)) {
            filename = cqlScriptResourceUrl.getPath();
        }

        LOG.debug("CQL script filename: {}", filename);

        assertNotNull(sqlConnection);
        final Statement stmt = sqlConnection.createStatement();
        stmt.execute(String.format("SOURCE '%s'", filename));
        final ResultSet rs = stmt.getResultSet();
        assertNotNull(rs);
        assertFalse(rs.next());
        rs.close();
        stmt.close();

        // Verify the rows to insert as specified in the executed script are effectively present.
        final PreparedStatement verifyStmt = sqlConnection.prepareStatement(
            "SELECT keyname, t1bValue, t1iValue FROM cf_test1 WHERE keyname = ?");
        verifyStmt.setString(1, "key100");
        ResultSet verifyRs = verifyStmt.executeQuery();
        assertTrue(verifyRs.next());
        assertEquals("key100", verifyRs.getString("keyname"));
        assertTrue(verifyRs.getBoolean("t1bValue"));
        assertEquals(100, verifyRs.getInt("t1iValue"));

        verifyStmt.setString(1, "key101");
        verifyRs = verifyStmt.executeQuery();
        assertTrue(verifyRs.next());
        assertEquals("key101", verifyRs.getString("keyname"));
        assertFalse(verifyRs.getBoolean("t1bValue"));
        assertEquals(101, verifyRs.getInt("t1iValue"));

        verifyStmt.setString(1, "key102");
        verifyRs = verifyStmt.executeQuery();
        assertTrue(verifyRs.next());
        assertEquals("key102", verifyRs.getString("keyname"));
        assertTrue(verifyRs.getBoolean("t1bValue"));
        assertEquals(102, verifyRs.getInt("t1iValue"));

        verifyRs.close();
        verifyStmt.close();
    }

    @Test
    void givenCqlFileWithErrors_whenExecuteSourceCommand_throwException() throws SQLException {
        assertNotNull(sqlConnection);
        sqlConnection.setOptionSet(new Default() {
            // Deactivate asynchronous execution of the multiple queries by statement to check the statements after a
            // faulty line are not executed in this case.
            @Override
            public boolean executeMultipleQueriesByStatementAsync() {
                return false;
            }
        });
        final Statement stmt = sqlConnection.createStatement();
        assertThrows(SQLTransientException.class, () -> stmt.execute(
            String.format("SOURCE '%s'", "src/test/resources/test_source_cmd_with_errors.cql"))
        );
        final ResultSet rs = stmt.getResultSet();
        assertNull(rs);

        stmt.close();

        // Verify the first row to insert (before the line throwing an error) as specified in the executed script is
        // effectively present, but not the one after the faulty line.
        final PreparedStatement verifyStmt = sqlConnection.prepareStatement(
            "SELECT keyname, t1bValue, t1iValue FROM cf_test1 WHERE keyname = ?");
        verifyStmt.setString(1, "key103");
        ResultSet verifyRs = verifyStmt.executeQuery();
        assertTrue(verifyRs.next());
        assertEquals("key103", verifyRs.getString("keyname"));
        assertTrue(verifyRs.getBoolean("t1bValue"));
        assertEquals(103, verifyRs.getInt("t1iValue"));

        verifyStmt.setString(1, "key104");
        verifyRs = verifyStmt.executeQuery();
        assertFalse(verifyRs.next());

        verifyRs.close();
        verifyStmt.close();

        // Reset compliance mode to the default value.
        sqlConnection.setOptionSet(new Default());
    }

}
