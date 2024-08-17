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

import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.PlainTextAuthProviderBase;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.ing.data.cassandra.jdbc.utils.AnotherFakeLoadBalancingPolicy;
import com.ing.data.cassandra.jdbc.utils.AnotherFakeRetryPolicy;
import com.ing.data.cassandra.jdbc.utils.ContactPoint;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.File;
import java.net.URL;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.ing.data.cassandra.jdbc.CassandraDataSource.DATA_SOURCE_DESCRIPTION;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_COMPLIANCE_MODE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONSISTENCY_LEVEL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
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
        ds.setActiveProfile("custom_profile");
        assertEquals("custom_profile", ds.getActiveProfile());
        ds.setRetryPolicy("com.ing.data.cassandra.jdbc.utils.FakeRetryPolicy");
        assertEquals("com.ing.data.cassandra.jdbc.utils.FakeRetryPolicy", ds.getRetryPolicy());
        ds.setLoadBalancingPolicy("com.ing.data.cassandra.jdbc.utils.FakeLoadBalancingPolicy");
        assertEquals("com.ing.data.cassandra.jdbc.utils.FakeLoadBalancingPolicy", ds.getLoadBalancingPolicy());
        ds.setReconnectionPolicy("com.ing.data.cassandra.jdbc.utils.FakeReconnectionPolicy()");
        assertEquals("com.ing.data.cassandra.jdbc.utils.FakeReconnectionPolicy()", ds.getReconnectionPolicy());
        ds.setFetchSize(500);
        assertEquals(500, ds.getFetchSize());
        ds.setRequestTimeout(5_000L);
        assertEquals(5_000, ds.getRequestTimeout());
        ds.setConnectionTimeout(10_000L);
        assertEquals(10_000, ds.getConnectionTimeout());
        ds.setSslEnabled(true);
        assertTrue(ds.isSslEnabled());
        assertTrue(ds.isHostnameVerified()); // true when SSL enabled with DefaultSslEngineFactory.
        ds.setSslEngineFactory("com.ing.data.cassandra.jdbc.utils.FakeSslEngineFactory");
        assertEquals("com.ing.data.cassandra.jdbc.utils.FakeSslEngineFactory", ds.getSslEngineFactory());
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
        assertConnectionHasExpectedConfig(connection);
        connection.close();
    }

    @Test
    void givenDataSourceWithUrl_whenConnect_returnCassandraConnection() throws Exception {
        final CassandraDataSource ds = new CassandraDataSource(CONTACT_POINTS, KEYSPACE);
        ds.setURL(buildJdbcUrl(CONTACT_POINT_HOST, CONTACT_POINT_PORT, KEYSPACE, "consistency=TWO", "fetchsize=5000",
            "localdatacenter=DC1", "loadbalancing=com.ing.data.cassandra.jdbc.utils.AnotherFakeLoadBalancingPolicy",
            "requesttimeout=8000", "retry=com.ing.data.cassandra.jdbc.utils.AnotherFakeRetryPolicy",
            "reconnection=ConstantReconnectionPolicy((long)10)", "connecttimeout=15000", "tcpnodelay=false",
            "keepalive=true", "user=testUser", "password=testPassword"));

        final CassandraConnection connection = ds.getConnection();
        assertConnectionHasExpectedConfig(connection);
        connection.close();
    }

    private void assertConnectionHasExpectedConfig(final CassandraConnection connection) {
        assertNotNull(connection);
        assertNotNull(connection.getSession());
        assertNotNull(connection.getSession().getContext());
        assertNotNull(connection.getSession().getContext().getConfig());
        assertNotNull(connection.getSession().getContext().getConfig().getDefaultProfile());

        final InternalDriverContext internalContext = (InternalDriverContext) connection.getSession().getContext();

        assertNotNull(connection.getConsistencyLevel());
        final ConsistencyLevel consistencyLevel = connection.getConsistencyLevel();
        assertNotNull(consistencyLevel);
        assertEquals(ConsistencyLevel.TWO, consistencyLevel);

        final int fetchSize = connection.getDefaultFetchSize();
        assertEquals(5000, fetchSize);

        final String localDC = connection.getSession().getContext().getConfig()
            .getDefaultProfile().getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER,
                internalContext.getLocalDatacenter(DriverExecutionProfile.DEFAULT_NAME));
        assertEquals("DC1", localDC);

        final Optional<AuthProvider> authProvider = connection.getSession().getContext().getAuthProvider();
        assertTrue(authProvider.isPresent());
        assertThat(authProvider.get(), instanceOf(PlainTextAuthProviderBase.class));
        if (authProvider.get() instanceof PlainTextAuthProvider) {
            assertEquals("testUser", connection.getSession().getContext().getConfig()
                .getDefaultProfile().getString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME));
            assertEquals("testPassword", connection.getSession().getContext().getConfig()
                .getDefaultProfile().getString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD));
        }

        assertEquals(Duration.ofSeconds(8), connection.getSession().getContext().getConfig()
            .getDefaultProfile().getDuration(DefaultDriverOption.REQUEST_TIMEOUT));

        final LoadBalancingPolicy loadBalancingPolicy = connection.getSession().getContext()
            .getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME);
        assertNotNull(loadBalancingPolicy);
        assertThat(loadBalancingPolicy, instanceOf(AnotherFakeLoadBalancingPolicy.class));

        final RetryPolicy retryPolicy = connection.getSession().getContext()
            .getRetryPolicy(DriverExecutionProfile.DEFAULT_NAME);
        assertNotNull(retryPolicy);
        assertThat(retryPolicy, instanceOf(AnotherFakeRetryPolicy.class));

        final ReconnectionPolicy reconnectionPolicy = connection.getSession().getContext().getReconnectionPolicy();
        assertNotNull(reconnectionPolicy);
        assertThat(reconnectionPolicy, instanceOf(ConstantReconnectionPolicy.class));
        assertEquals(Duration.ofSeconds(10), reconnectionPolicy.newControlConnectionSchedule(false).nextDelay());

        final DriverExecutionProfile driverConfigDefaultProfile =
            connection.getSession().getContext().getConfig().getDefaultProfile();
        assertEquals(Duration.ofSeconds(15),
            driverConfigDefaultProfile.getDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT));
        assertFalse(driverConfigDefaultProfile.getBoolean(DefaultDriverOption.SOCKET_TCP_NODELAY));
        assertTrue(driverConfigDefaultProfile.getBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE));

        // Check the not overridden values.
        assertTrue(connection.getSession().getKeyspace().isPresent());
        assertEquals(KEYSPACE, connection.getSession().getKeyspace().get().asCql(true));
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

    @Test
    @Deprecated
    void givenParameters_whenConstructDataSourceWithDeprecatedConstructors_returnCassandraDataSource() throws Exception {
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
}
