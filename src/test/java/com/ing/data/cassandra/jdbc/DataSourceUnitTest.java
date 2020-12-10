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

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLFeatureNotSupportedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataSourceUnitTest extends UsingEmbeddedCassandraServerTest {

    private static final String KEYSPACE = "test_keyspace";
    private static final String USER = "testuser";
    private static final String PASSWORD = "secret";
    private static final String VERSION = "3.0.0";
    private static final String CONSISTENCY = "ONE";

    @Test
    void givenParameters_whenConstructDataSource_returnCassandraDataSource() throws Exception {
        final CassandraDataSource cds = new CassandraDataSource(BuildCassandraServer.HOST, BuildCassandraServer.PORT,
            KEYSPACE, USER, PASSWORD, VERSION, CONSISTENCY);
        assertEquals(BuildCassandraServer.HOST, cds.getServerName());
        assertEquals(BuildCassandraServer.PORT, cds.getPortNumber());
        assertEquals(KEYSPACE, cds.getDatabaseName());
        assertEquals(USER, cds.getUser());
        assertEquals(PASSWORD, cds.getPassword());
        assertEquals(VERSION, cds.getVersion());

        final DataSource ds = new CassandraDataSource(BuildCassandraServer.HOST, BuildCassandraServer.PORT, KEYSPACE,
            USER, PASSWORD, VERSION, CONSISTENCY);
        assertNotNull(ds);

        // null username and password
        java.sql.Connection cnx = ds.getConnection(null, null);
        assertFalse(cnx.isClosed());
        ds.setLoginTimeout(5);
        assertEquals(5, ds.getLoginTimeout());

        // no username and password
        cnx = ds.getConnection();
        assertFalse(cnx.isClosed());
        ds.setLoginTimeout(5);
        assertEquals(VERSION, ((CassandraConnection) cnx).getConnectionProps().get(Utils.TAG_CQL_VERSION));
        assertEquals(5, ds.getLoginTimeout());
    }


    @Test
    void givenCassandraDataSource_whenIsWrapperFor_returnExpectedValue() throws Exception {
        final DataSource ds = new CassandraDataSource(BuildCassandraServer.HOST, BuildCassandraServer.PORT, KEYSPACE,
            USER, PASSWORD, VERSION, CONSISTENCY);

        // Assert it is a wrapper for DataSource.
        assertTrue(ds.isWrapperFor(DataSource.class));

        // Assert it is not a wrapper for this test class.
        assertFalse(ds.isWrapperFor(this.getClass()));
    }

    @Test
    void givenCassandraDataSource_whenUnwrap_returnUnwrappedDatasource() throws Exception {
        final DataSource ds = new CassandraDataSource(BuildCassandraServer.HOST, BuildCassandraServer.PORT, KEYSPACE,
            USER, PASSWORD, VERSION, CONSISTENCY);
        assertNotNull(ds.unwrap(DataSource.class));
    }

    @Test
    void givenCassandraDataSource_whenUnwrapToInvalidInterface_returnUnwrappedDatasource() {
        final DataSource ds = new CassandraDataSource(BuildCassandraServer.HOST, BuildCassandraServer.PORT, KEYSPACE,
            USER, PASSWORD, VERSION, CONSISTENCY);
        assertThrows(SQLFeatureNotSupportedException.class, () -> ds.unwrap(this.getClass()));
    }
}
