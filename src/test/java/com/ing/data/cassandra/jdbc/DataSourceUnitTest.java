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

import com.ing.data.cassandra.jdbc.utils.ContactPoint;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;

import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_COMPLIANCE_MODE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONSISTENCY_LEVEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("deprecation")
class DataSourceUnitTest extends UsingCassandraContainerTest {

    private static final String KEYSPACE = "test_keyspace";
    private static final String USER = "testuser";
    private static final String PASSWORD = "secret";
    private static final String CONSISTENCY = "ONE";
    private static final String COMPLIANCE_MODE = "Liquibase";

    @Test
    void givenParameters_whenConstructDataSource_returnCassandraDataSource() throws Exception {
        final CassandraDataSource cds = new CassandraDataSource(
            Collections.singletonList(ContactPoint.of("localhost", 9042)), KEYSPACE, USER,
            PASSWORD, CONSISTENCY, "datacenter1");
        assertNotNull(cds.getContactPoints());
        assertEquals(1, cds.getContactPoints().size());
        final ContactPoint dsContactPoint = cds.getContactPoints().get(0);
        assertEquals("localhost", dsContactPoint.getHost());
        assertEquals(9042, dsContactPoint.getPort());
        assertEquals(KEYSPACE, cds.getDatabaseName());
        assertEquals(USER, cds.getUser());
        assertEquals(PASSWORD, cds.getPassword());

        final CassandraDataSource ds = new CassandraDataSource(Collections.singletonList(ContactPoint.of(
            cassandraContainer.getContactPoint().getHostName(), cassandraContainer.getContactPoint().getPort())),
            KEYSPACE, USER, PASSWORD, CONSISTENCY, "datacenter1");
        ds.setComplianceMode(COMPLIANCE_MODE);
        assertNotNull(ds);

        // null username and password
        CassandraConnection cnx = ds.getConnection(null, null);
        assertFalse(cnx.isClosed());
        ds.setLoginTimeout(5);
        assertEquals(5, ds.getLoginTimeout());

        // no username and password
        cnx = ds.getConnection();
        assertFalse(cnx.isClosed());
        ds.setLoginTimeout(5);
        assertEquals(CONSISTENCY, cnx.getConnectionProperties().get(TAG_CONSISTENCY_LEVEL));
        assertEquals(COMPLIANCE_MODE, cnx.getConnectionProperties().get(TAG_COMPLIANCE_MODE));

        assertEquals(5, ds.getLoginTimeout());
    }

    @Test
    void givenCassandraDataSource_whenIsWrapperFor_returnExpectedValue() throws Exception {
        final DataSource ds =new CassandraDataSource(Collections.singletonList(ContactPoint.of(
            cassandraContainer.getContactPoint().getHostName(), cassandraContainer.getContactPoint().getPort())),
            KEYSPACE, USER, PASSWORD, CONSISTENCY);

        // Assert it is a wrapper for DataSource.
        assertTrue(ds.isWrapperFor(DataSource.class));

        // Assert it is not a wrapper for this test class.
        assertFalse(ds.isWrapperFor(this.getClass()));
    }

    @Test
    void givenCassandraDataSource_whenUnwrap_returnUnwrappedDatasource() throws Exception {
        final DataSource ds =new CassandraDataSource(Collections.singletonList(ContactPoint.of(
            cassandraContainer.getContactPoint().getHostName(), cassandraContainer.getContactPoint().getPort())),
            KEYSPACE, USER, PASSWORD, CONSISTENCY);
        assertNotNull(ds.unwrap(DataSource.class));
    }

    @Test
    void givenCassandraDataSource_whenUnwrapToInvalidInterface_throwException() {
        final DataSource ds = new CassandraDataSource(Collections.singletonList(ContactPoint.of(
            cassandraContainer.getContactPoint().getHostName(), cassandraContainer.getContactPoint().getPort())),
            KEYSPACE, USER, PASSWORD, CONSISTENCY);
        assertThrows(SQLException.class, () -> ds.unwrap(this.getClass()));
    }
}
