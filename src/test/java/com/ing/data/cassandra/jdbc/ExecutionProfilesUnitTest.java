/*
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

import com.datastax.driver.core.SimpleStatement;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class ExecutionProfilesUnitTest extends UsingCassandraContainerTest {

    private static final String KEYSPACE = "system";

    @Test
    void givenCassandraConnectionAndSpecificProfile_whenExecuteStatement_useExpectedProfile() throws Exception {
        final String customProfileName = "customProfile";
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<com.datastax.oss.driver.api.core.cql.Statement<?>> stmtCaptor =
            ArgumentCaptor.forClass(com.datastax.oss.driver.api.core.cql.Statement.class);
        final URL confTestUrl = this.getClass().getClassLoader().getResource("test_application_multiprofiles.conf");
        if (confTestUrl == null) {
            fail("Unable to find test_application_multiprofiles.conf");
        }
        initConnection(KEYSPACE, "configfile=" + confTestUrl.getPath());

        final CqlSession spiedSession = spy((CqlSession) sqlConnection.getSession());
        final CassandraConnection jdbcConnection = new CassandraConnection(spiedSession, KEYSPACE,
            sqlConnection.getConsistencyLevel(), false, null, null);

        // Execution profile never changed, so it should remain the default one when restoring the last one.
        jdbcConnection.restoreLastExecutionProfile();
        assertEquals(DriverExecutionProfile.DEFAULT_NAME, jdbcConnection.getActiveExecutionProfile().getName());

        // Execute query with default profile.
        jdbcConnection.createStatement().executeQuery("SELECT release_version FROM system.local");
        verify(spiedSession, atLeastOnce()).execute(stmtCaptor.capture());
        com.datastax.oss.driver.api.core.cql.Statement<?> executedStmt = stmtCaptor.getValue();
        assertNotNull(executedStmt.getExecutionProfile());
        assertEquals(DriverExecutionProfile.DEFAULT_NAME, executedStmt.getExecutionProfile().getName());

        // Switch to another profile.
        jdbcConnection.setActiveExecutionProfile(customProfileName);
        assertEquals(customProfileName, jdbcConnection.getActiveExecutionProfile().getName());

        jdbcConnection.createStatement().executeQuery("SELECT release_version FROM system.local");
        verify(spiedSession, atLeastOnce()).execute(stmtCaptor.capture());
        executedStmt = stmtCaptor.getValue();
        assertNotNull(executedStmt.getExecutionProfile());
        assertEquals(customProfileName, executedStmt.getExecutionProfile().getName());

        // Switch to an invalid profile shouldn't change the active profile.
        jdbcConnection.setActiveExecutionProfile("invalid_profile");
        assertEquals(customProfileName, jdbcConnection.getActiveExecutionProfile().getName());

        // Restore the last active execution profile.
        jdbcConnection.restoreLastExecutionProfile();
        assertEquals(DriverExecutionProfile.DEFAULT_NAME, jdbcConnection.getActiveExecutionProfile().getName());
    }

}
