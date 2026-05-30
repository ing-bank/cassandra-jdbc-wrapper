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
import java.io.File;
import java.net.URL;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static com.ing.data.cassandra.jdbc.CassandraDataSource.DATA_SOURCE_DESCRIPTION;
import static com.ing.data.cassandra.jdbc.testing.AssertionsUtils.assertConnectionHasExpectedConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class DataSourceUnitTest extends UsingCassandraContainerTest {

    private static final List<ContactPoint> UNREACHABLE_CONTACT_POINTS = Collections.singletonList(
        ContactPoint.of("localhost", 9042));
    private static final String CONTACT_POINT_HOST = cassandraContainer.getContactPoint().getHostName();
    private static final int CONTACT_POINT_PORT = cassandraContainer.getContactPoint().getPort();
    private static final List<ContactPoint> CONTACT_POINTS = Collections.singletonList(ContactPoint.of(
        CONTACT_POINT_HOST, CONTACT_POINT_PORT));
    private static final String KEYSPACE = "test_keyspace";
    private static final String LOCAL_DC = "datacenter1";
    private static final String USER = "testuser";
    private static final String PASSWORD = "secret";
    private static final String CONSISTENCY = "ONE";
    private static final String SERIAL_CONSISTENCY = "LOCAL_SERIAL";
    private static final String COMPLIANCE_MODE = "Liquibase";

    @Test
    void givenParameters_whenConstructDataSource_returnCassandraDataSource() {
        final CassandraDataSource ds = new CassandraDataSource(UNREACHABLE_CONTACT_POINTS, KEYSPACE);

        // Check values set in the constructor.
        assertNotNull(ds.getContactPoints());
        assertEquals(1, ds.getContactPoints().size());
        final ContactPoint dsContactPoint = ds.getContactPoints().get(0);
        assertEquals("localhost", dsContactPoint.getHost());
        assertEquals(9042, dsContactPoint.getPort());
        assertEquals(KEYSPACE, ds.getDatabaseName());

        // Check default values.
        assertEquals(DATA_SOURCE_DESCRIPTION, ds.getDescription());
        assertEquals("Default", ds.getComplianceMode());
        assertEquals("LOCAL_ONE", ds.getConsistency());
        assertEquals("SERIAL", ds.getSerialConsistency());
        assertEquals("default", ds.getActiveProfile());
        assertEquals("DefaultRetryPolicy", ds.getRetryPolicy());
        assertEquals("DefaultLoadBalancingPolicy", ds.getLoadBalancingPolicy());
        assertEquals("ExponentialReconnectionPolicy()", ds.getReconnectionPolicy());
        assertEquals("DefaultSslEngineFactory", ds.getSslEngineFactory());
        assertEquals(100, ds.getFetchSize());
        assertEquals(2_000, ds.getRequestTimeout());
        assertEquals(5_000, ds.getConnectionTimeout());
        assertFalse(ds.isSslEnabled());
        assertFalse(ds.isHostnameVerified());
        assertFalse(ds.isKerberosAuthProviderEnabled());
        assertFalse(ds.isTcpKeepAliveEnabled());
        assertTrue(ds.isTcpNoDelayEnabled());
        assertNull(ds.getUser());
        assertNull(ds.getPassword());
        assertNull(ds.getLocalDataCenter());
        assertNull(ds.getSecureConnectBundle());
        assertNull(ds.getConfigurationFile());

        // Set custom values and check them.
        ds.setComplianceMode("Liquibase");
        assertEquals("Liquibase", ds.getComplianceMode());
        ds.setConsistency(CONSISTENCY);
        assertEquals(CONSISTENCY, ds.getConsistency());
        ds.setSerialConsistency(SERIAL_CONSISTENCY);
        assertEquals(SERIAL_CONSISTENCY, ds.getSerialConsistency());
        ds.setActiveProfile("custom_profile");
        assertEquals("custom_profile", ds.getActiveProfile());
        ds.setRetryPolicy("com.ing.data.cassandra.jdbc.testing.FakeRetryPolicy");
        assertEquals("com.ing.data.cassandra.jdbc.testing.FakeRetryPolicy", ds.getRetryPolicy());
        ds.setLoadBalancingPolicy("com.ing.data.cassandra.jdbc.testing.FakeLoadBalancingPolicy");
        assertEquals("com.ing.data.cassandra.jdbc.testing.FakeLoadBalancingPolicy", ds.getLoadBalancingPolicy());
        ds.setReconnectionPolicy("com.ing.data.cassandra.jdbc.testing.FakeReconnectionPolicy()");
        assertEquals("com.ing.data.cassandra.jdbc.testing.FakeReconnectionPolicy()", ds.getReconnectionPolicy());
        ds.setFetchSize(500);
        assertEquals(500, ds.getFetchSize());
        ds.setRequestTimeout(5_000L);
        assertEquals(5_000, ds.getRequestTimeout());
        ds.setConnectionTimeout(10_000L);
        assertEquals(10_000, ds.getConnectionTimeout());
        ds.setSslEnabled(true);
        assertTrue(ds.isSslEnabled());
        assertTrue(ds.isHostnameVerified()); // true when SSL enabled with DefaultSslEngineFactory.
        ds.setSslEngineFactory("com.ing.data.cassandra.jdbc.testing.FakeSslEngineFactory");
        assertEquals("com.ing.data.cassandra.jdbc.testing.FakeSslEngineFactory", ds.getSslEngineFactory());
        assertFalse(ds.isHostnameVerified()); // false when SSL enabled with custom SslEngineFactory.
        ds.setHostnameVerified(true);
        assertTrue(ds.isHostnameVerified());
        ds.setKerberosAuthProviderEnabled(true);
        assertTrue(ds.isKerberosAuthProviderEnabled());
        ds.setTcpKeepAliveEnabled(true);
        assertTrue (ds.isTcpKeepAliveEnabled());
        ds.setTcpNoDelayEnabled(false);
        assertFalse(ds.isTcpNoDelayEnabled());
        ds.setUser(USER);
        assertEquals(USER, ds.getUser());
        ds.setPassword(PASSWORD);
        assertEquals(PASSWORD, ds.getPassword());
        ds.setLocalDataCenter(LOCAL_DC);
        assertEquals(LOCAL_DC, ds.getLocalDataCenter());
        ds.setSecureConnectBundle("path/to/bundle.zip");
        assertEquals("path/to/bundle.zip", ds.getSecureConnectBundle());
        ds.setConfigurationFile("path/to/cassandra.conf");
        assertEquals("path/to/cassandra.conf", ds.getConfigurationFile());
    }

    @Test
    void givenDataSourceWithDefaultParameters_whenConnect_returnCassandraConnection() throws Exception {
        final CassandraDataSource ds = new CassandraDataSource(CONTACT_POINTS, KEYSPACE);
        ds.setLocalDataCenter(LOCAL_DC);

        // With null username and password.
        CassandraConnection connection = ds.getConnection(null, null);
        assertFalse(connection.isClosed());
        ds.setLoginTimeout(5);
        assertEquals(5, ds.getLoginTimeout());
        connection.close();

        // Without specifying username and password.
        connection = ds.getConnection();
        assertFalse(connection.isClosed());
        assertEquals(5, ds.getLoginTimeout());
        connection.close();
    }

    @Test
    void givenDataSourceWithSpecificParameters_whenConnect_returnCassandraConnection() throws Exception {
        final URL confTestUrl = this.getClass().getClassLoader().getResource("test_application.conf");
        if (confTestUrl == null) {
            fail("Unable to find test_application.conf");
        }
        final CassandraDataSource ds = new CassandraDataSource(CONTACT_POINTS, KEYSPACE);
        ds.setConfigurationFile(new File(confTestUrl.toURI()).toPath());

        final CassandraConnection connection = ds.getConnection();
        assertConnectionHasExpectedConfig(connection, KEYSPACE);
        connection.close();
    }

    @Test
    void givenDataSourceWithUrl_whenConnect_returnCassandraConnection() throws Exception {
        final CassandraDataSource ds = new CassandraDataSource(CONTACT_POINTS, KEYSPACE);
        ds.setURL(buildJdbcUrl(CONTACT_POINT_HOST, CONTACT_POINT_PORT, KEYSPACE, "consistency=TWO",
            "serialconsistency=LOCAL_SERIAL", "fetchsize=5000", "localdatacenter=DC1",
            "loadbalancing=com.ing.data.cassandra.jdbc.testing.AnotherFakeLoadBalancingPolicy",
            "requesttimeout=8000", "retry=com.ing.data.cassandra.jdbc.testing.AnotherFakeRetryPolicy",
            "reconnection=ConstantReconnectionPolicy((long)10)", "connecttimeout=15000", "tcpnodelay=false",
            "keepalive=true", "user=testUser", "password=testPassword"));

        final CassandraConnection connection = ds.getConnection();
        assertConnectionHasExpectedConfig(connection, KEYSPACE);
        connection.close();
    }

    @Test
    void givenDataSource_whenBuildConnectionBuilder_returnCassandraConnection() throws Exception {
        final CassandraDataSource ds = new CassandraDataSource(null, null);
        final CassandraConnectionBuilder connectionBuilder = (CassandraConnectionBuilder) ds.createConnectionBuilder();
        final CassandraConnection connection = (CassandraConnection) connectionBuilder
            .user("testUser")
            .password("testPassword")
            .contactPoints(CONTACT_POINTS)
            .databaseName(KEYSPACE)
            .consistency("TWO")
            .serialConsistency("LOCAL_SERIAL")
            .fetchSize(5_000)
            .localDataCenter("DC1")
            .loadBalancingPolicy("com.ing.data.cassandra.jdbc.testing.AnotherFakeLoadBalancingPolicy")
            .requestTimeout(8_000L)
            .retryPolicy("com.ing.data.cassandra.jdbc.testing.AnotherFakeRetryPolicy")
            .reconnectionPolicy("ConstantReconnectionPolicy((long)10)")
            .connectionTimeout(15_000L)
            .tcpNoDelayEnabled(false)
            .tcpKeepAliveEnabled(true)
            .build();
        assertConnectionHasExpectedConfig(connection, KEYSPACE);
        connection.close();
    }

    @Test
    void givenCassandraDataSource_whenIsWrapperFor_returnExpectedValue() throws Exception {
        final DataSource ds = new CassandraDataSource(CONTACT_POINTS, KEYSPACE);
        // Assert it is a wrapper for DataSource.
        assertTrue(ds.isWrapperFor(DataSource.class));

        // Assert it is not a wrapper for this test class.
        assertFalse(ds.isWrapperFor(this.getClass()));
    }

    @Test
    void givenCassandraDataSource_whenUnwrap_returnUnwrappedDatasource() throws Exception {
        final DataSource ds = new CassandraDataSource(CONTACT_POINTS, KEYSPACE);
        assertNotNull(ds.unwrap(DataSource.class));
    }

    @Test
    void givenCassandraDataSource_whenUnwrapToInvalidInterface_throwException() {
        final DataSource ds = new CassandraDataSource(CONTACT_POINTS, KEYSPACE);
        assertThrows(SQLException.class, () -> ds.unwrap(this.getClass()));
    }

}
