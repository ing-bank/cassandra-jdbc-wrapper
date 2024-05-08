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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.ing.data.cassandra.jdbc.optionset.Liquibase;
import com.ing.data.cassandra.jdbc.utils.AnotherFakeLoadBalancingPolicy;
import com.ing.data.cassandra.jdbc.utils.AnotherFakeRetryPolicy;
import com.ing.data.cassandra.jdbc.utils.FakeLoadBalancingPolicy;
import com.ing.data.cassandra.jdbc.utils.FakeReconnectionPolicy;
import com.ing.data.cassandra.jdbc.utils.FakeRetryPolicy;
import com.ing.data.cassandra.jdbc.utils.FakeSslEngineFactory;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import static com.ing.data.cassandra.jdbc.SessionHolder.URL_KEY;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.JSSE_KEYSTORE_PASSWORD_PROPERTY;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.JSSE_KEYSTORE_PROPERTY;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.JSSE_TRUSTSTORE_PASSWORD_PROPERTY;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.JSSE_TRUSTSTORE_PROPERTY;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_TIMEOUT;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.SSL_CONFIG_FAILED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConnectionUnitTest extends UsingCassandraContainerTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionUnitTest.class);

    private static final String KEYSPACE = "system";

    @Test
    void givenInvalidConfigurationFile_whenGetConnection_createConnectionIgnoringConfigFile() throws Exception {
        initConnection(KEYSPACE, "configfile=wrong_application.conf", "consistency=LOCAL_QUORUM",
            "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getConsistencyLevel());
        final ConsistencyLevel consistencyLevel = sqlConnection.getConsistencyLevel();
        assertNotNull(consistencyLevel);
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, consistencyLevel);
        sqlConnection.close();
    }

    @Test
    void givenInvalidFetchSize_whenGetConnection_createConnectionWithFallbackFetchSize() throws Exception {
        initConnection(KEYSPACE, "fetchsize=NaN", "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        final int fetchSize = sqlConnection.getDefaultFetchSize();
        assertEquals(5000, fetchSize);
        sqlConnection.close();
    }

    @Test
    void givenFetchSize_whenGetConnection_createConnectionWithExpectedFetchSize() throws Exception {
        initConnection(KEYSPACE, "fetchsize=2000", "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        final int fetchSize = sqlConnection.getDefaultFetchSize();
        assertEquals(2000, fetchSize);
        sqlConnection.close();
    }

    @Test
    void givenNoLocalDataCenter_whenInferringLoadBalancingPolicySpecified_createConnectionWithExpectedConfig()
        throws Exception {
        initConnection(KEYSPACE, "loadBalancing=DcInferringLoadBalancingPolicy");
        assertNotNull(sqlConnection);
        final Statement statement = sqlConnection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT * FROM system.local");
        assertNotNull(resultSet);
        resultSet.close();
        statement.close();
        sqlConnection.close();
    }

    @Test
    void givenValidConfigurationFile_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        final URL confTestUrl = this.getClass().getClassLoader().getResource("test_application.conf");
        if (confTestUrl == null) {
            fail("Unable to find test_application.conf");
        }
        initConnection(KEYSPACE, "configfile=" + confTestUrl.getPath(), "localdatacenter=DC2",
            "user=aTestUser", "password=aTestPassword", "requesttimeout=5000",
            "connectimeout=8000", "keepalive=false", "tcpnodelay=true", "fetchsize=2000",
            "loadbalancing=com.ing.data.cassandra.jdbc.utils.FakeLoadBalancingPolicy",
            "retry=com.ing.data.cassandra.jdbc.utils.FakeRetryPolicy",
            "reconnection=com.ing.data.cassandra.jdbc.utils.FakeReconnectionPolicy()",
            "sslenginefactory=com.ing.data.cassandra.jdbc.utils.FakeSslEngineFactory");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        assertNotNull(sqlConnection.getSession().getContext().getConfig());
        assertNotNull(sqlConnection.getSession().getContext().getConfig().getDefaultProfile());
        assertNotNull(sqlConnection.getConsistencyLevel());
        final ConsistencyLevel consistencyLevel = sqlConnection.getConsistencyLevel();
        assertNotNull(consistencyLevel);
        assertEquals(ConsistencyLevel.TWO, consistencyLevel);

        final int fetchSize = sqlConnection.getDefaultFetchSize();
        assertEquals(5000, fetchSize);

        final String localDC = sqlConnection.getSession().getContext().getConfig()
            .getDefaultProfile().getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER);
        assertEquals("DC1", localDC);

        final Optional<AuthProvider> authProvider = sqlConnection.getSession().getContext().getAuthProvider();
        assertTrue(authProvider.isPresent());
        assertThat(authProvider.get(), instanceOf(PlainTextAuthProvider.class));
        assertEquals("testUser", sqlConnection.getSession().getContext().getConfig()
            .getDefaultProfile().getString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME));
        assertEquals("testPassword", sqlConnection.getSession().getContext().getConfig()
            .getDefaultProfile().getString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD));
        assertEquals(Duration.ofSeconds(8), sqlConnection.getSession().getContext().getConfig()
            .getDefaultProfile().getDuration(DefaultDriverOption.REQUEST_TIMEOUT));

        final LoadBalancingPolicy loadBalancingPolicy = sqlConnection.getSession().getContext()
            .getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME);
        assertNotNull(loadBalancingPolicy);
        assertThat(loadBalancingPolicy, instanceOf(AnotherFakeLoadBalancingPolicy.class));

        final RetryPolicy retryPolicy = sqlConnection.getSession().getContext()
            .getRetryPolicy(DriverExecutionProfile.DEFAULT_NAME);
        assertNotNull(retryPolicy);
        assertThat(retryPolicy, instanceOf(AnotherFakeRetryPolicy.class));

        final ReconnectionPolicy reconnectionPolicy = sqlConnection.getSession().getContext().getReconnectionPolicy();
        assertNotNull(reconnectionPolicy);
        assertThat(reconnectionPolicy, instanceOf(ConstantReconnectionPolicy.class));
        assertEquals(Duration.ofSeconds(10), reconnectionPolicy.newControlConnectionSchedule(false).nextDelay());

        final DriverExecutionProfile driverConfigDefaultProfile =
            sqlConnection.getSession().getContext().getConfig().getDefaultProfile();
        assertEquals(Duration.ofSeconds(15),
            driverConfigDefaultProfile.getDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT));
        assertFalse(driverConfigDefaultProfile.getBoolean(DefaultDriverOption.SOCKET_TCP_NODELAY));
        assertTrue(driverConfigDefaultProfile.getBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE));

        // Check the not overridden values.
        assertTrue(sqlConnection.getSession().getKeyspace().isPresent());
        assertEquals(KEYSPACE, sqlConnection.getSession().getKeyspace().get().asCql(true));
        sqlConnection.close();
    }

    @Test
    void givenRequestTimeout_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        initConnection(KEYSPACE, "requesttimeout=10000", "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        assertNotNull(sqlConnection.getSession().getContext().getConfig());
        assertEquals(Duration.ofSeconds(10), sqlConnection.getSession().getContext().getConfig()
            .getDefaultProfile().getDuration(DefaultDriverOption.REQUEST_TIMEOUT));
        sqlConnection.close();
    }

    @Test
    void givenConnectTimeout_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        initConnection(KEYSPACE, "connecttimeout=8000", "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        assertNotNull(sqlConnection.getSession().getContext().getConfig());
        assertEquals(Duration.ofSeconds(8), sqlConnection.getSession().getContext().getConfig()
            .getDefaultProfile().getDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT));
        // Check that when other socket options are not specified, the default values are used.
        assertFalse(sqlConnection.getSession().getContext().getConfig()
            .getDefaultProfile().getBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE));
        assertTrue(sqlConnection.getSession().getContext().getConfig()
            .getDefaultProfile().getBoolean(DefaultDriverOption.SOCKET_TCP_NODELAY));
        sqlConnection.close();
    }

    @Test
    void givenNonDefaultSocketOptions_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        initConnection(KEYSPACE, "tcpnodelay=false", "keepalive=true", "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        assertNotNull(sqlConnection.getSession().getContext().getConfig());
        assertTrue(sqlConnection.getSession().getContext().getConfig()
            .getDefaultProfile().getBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE));
        assertFalse(sqlConnection.getSession().getContext().getConfig()
            .getDefaultProfile().getBoolean(DefaultDriverOption.SOCKET_TCP_NODELAY));
        sqlConnection.close();
    }

    @Test
    void givenNoLoadBalancingPolicy_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        final LoadBalancingPolicy loadBalancingPolicy = sqlConnection.getSession().getContext()
            .getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME);
        assertNotNull(loadBalancingPolicy);
        assertThat(loadBalancingPolicy, instanceOf(DefaultLoadBalancingPolicy.class));
        sqlConnection.close();
    }

    @Test
    void givenCustomLoadBalancingPolicy_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        initConnection(KEYSPACE, "loadbalancing=com.ing.data.cassandra.jdbc.utils.FakeLoadBalancingPolicy");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        final LoadBalancingPolicy loadBalancingPolicy = sqlConnection.getSession().getContext()
            .getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME);
        assertNotNull(loadBalancingPolicy);
        assertThat(loadBalancingPolicy, instanceOf(FakeLoadBalancingPolicy.class));
        sqlConnection.close();
    }

    @Test
    void givenInvalidLoadBalancingPolicy_whenGetConnection_throwsException() {
        final Exception thrownException = assertThrows(Exception.class, () ->
            initConnection(KEYSPACE, "loadbalancing=FakeLoadBalancingPolicy"));
        assertThat(thrownException.getCause(), instanceOf(IllegalArgumentException.class));
    }

    @Test
    void givenNoRetryPolicy_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        final RetryPolicy retryPolicy = sqlConnection.getSession().getContext()
            .getRetryPolicy(DriverExecutionProfile.DEFAULT_NAME);
        assertNotNull(retryPolicy);
        assertThat(retryPolicy, instanceOf(DefaultRetryPolicy.class));
        sqlConnection.close();
    }

    @Test
    void givenCustomRetryPolicy_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        initConnection(KEYSPACE, "retry=com.ing.data.cassandra.jdbc.utils.FakeRetryPolicy", "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        final RetryPolicy retryPolicy = sqlConnection.getSession().getContext()
            .getRetryPolicy(DriverExecutionProfile.DEFAULT_NAME);
        assertNotNull(retryPolicy);
        assertThat(retryPolicy, instanceOf(FakeRetryPolicy.class));
        sqlConnection.close();
    }

    @Test
    void givenInvalidRetryPolicy_whenGetConnection_throwsException() {
        final Exception thrownException = assertThrows(Exception.class, () ->
            initConnection(KEYSPACE, "retry=FakeRetryPolicy"));
        assertThat(thrownException.getCause(), instanceOf(IllegalArgumentException.class));
    }

    @Test
    void givenConstantReconnectionPolicy_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        initConnection(KEYSPACE, "reconnection=ConstantReconnectionPolicy((long)10)", "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        final ReconnectionPolicy reconnectionPolicy = sqlConnection.getSession().getContext().getReconnectionPolicy();
        assertNotNull(reconnectionPolicy);
        assertThat(reconnectionPolicy, instanceOf(ConstantReconnectionPolicy.class));
        assertEquals(reconnectionPolicy.newControlConnectionSchedule(false).nextDelay(), Duration.ofSeconds(10));
        sqlConnection.close();
    }

    @Test
    void givenExponentialReconnectionPolicy_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        initConnection(KEYSPACE, "reconnection=ExponentialReconnectionPolicy((long)10,(long)100)", "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        final ReconnectionPolicy reconnectionPolicy = sqlConnection.getSession().getContext().getReconnectionPolicy();
        assertNotNull(reconnectionPolicy);
        assertThat(reconnectionPolicy, instanceOf(ExponentialReconnectionPolicy.class));
        assertEquals(((ExponentialReconnectionPolicy) reconnectionPolicy).getBaseDelayMs(), 10_000L);
        assertEquals(((ExponentialReconnectionPolicy) reconnectionPolicy).getMaxDelayMs(), 100_000L);
        sqlConnection.close();
    }

    @Test
    void givenCustomReconnectionPolicy_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        initConnection(KEYSPACE, "reconnection=com.ing.data.cassandra.jdbc.utils.FakeReconnectionPolicy()",
            "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        final ReconnectionPolicy reconnectionPolicy = sqlConnection.getSession().getContext().getReconnectionPolicy();
        assertNotNull(reconnectionPolicy);
        assertThat(reconnectionPolicy, instanceOf(FakeReconnectionPolicy.class));
        sqlConnection.close();
    }

    @Test
    void givenInvalidReconnectionPolicy_whenGetConnection_createConnectionWithDefaultPolicyConfig() throws Exception {
        initConnection(KEYSPACE, "reconnection=ExponentialReconnectionPolicy((int)100)", "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        final ReconnectionPolicy reconnectionPolicy = sqlConnection.getSession().getContext().getReconnectionPolicy();
        assertNotNull(reconnectionPolicy);
        assertThat(reconnectionPolicy, instanceOf(ExponentialReconnectionPolicy.class));
        // Check that the default configuration of the policy is used.
        assertEquals(((ExponentialReconnectionPolicy) reconnectionPolicy).getBaseDelayMs(), 1_000L);
        assertEquals(((ExponentialReconnectionPolicy) reconnectionPolicy).getMaxDelayMs(), 60_000L);
        sqlConnection.close();
    }

    @Test
    void givenInvalidReconnectionPolicy_whenGetConnectionInDebugMode_throwsException() {
        final Exception thrownException = assertThrows(Exception.class, () ->
            initConnection(KEYSPACE, "debug=true&reconnection=FakeReconnectionPolicy()"));
        assertThat(thrownException.getCause(), instanceOf(IllegalArgumentException.class));
    }

    @Test
    void givenDisabledSsl_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        initConnection(KEYSPACE, "enablessl=false", "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        assertFalse(sqlConnection.getSession().getContext().getSslEngineFactory().isPresent());
        sqlConnection.close();
    }

    @Test
    void givenInvalidSslEngineFactory_whenGetConnection_throwsException() {
        final SQLNonTransientConnectionException thrownException =
            assertThrows(SQLNonTransientConnectionException.class,
                () -> initConnection(KEYSPACE, "sslenginefactory=com.ing.data.InvalidSslEngineFactory"));
        assertThat(thrownException.getCause(), instanceOf(ClassNotFoundException.class));
        assertThat(thrownException.getMessage(),
            Matchers.startsWith(SSL_CONFIG_FAILED.substring(0, SSL_CONFIG_FAILED.indexOf(":"))));
    }

    /*
     * IMPORTANT NOTE:
     * The resources 'cassandra.keystore' and 'cassandra.truststore' are provided for testing purpose only. They contain
     * self-signed certificate to not use in a production context.
     */

    @Test
    void givenEnabledSslWithJsse_whenConfigureSsl_addDefaultSslEngineFactoryToSessionBuilder() throws Exception {
        final ClassLoader classLoader = this.getClass().getClassLoader();
        System.setProperty(JSSE_TRUSTSTORE_PROPERTY,
            Objects.requireNonNull(classLoader.getResource("cassandra.truststore")).getPath());
        System.setProperty(JSSE_TRUSTSTORE_PASSWORD_PROPERTY, "changeit");
        System.setProperty(JSSE_KEYSTORE_PROPERTY,
            Objects.requireNonNull(classLoader.getResource("cassandra.keystore")).getPath());
        System.setProperty(JSSE_KEYSTORE_PASSWORD_PROPERTY, "changeit");

        initConnection(KEYSPACE, "enablessl=true", "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        assertTrue(sqlConnection.getSession().getContext().getSslEngineFactory().isPresent());

        final Statement statement = sqlConnection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT * FROM system.local");
        assertNotNull(resultSet);
        resultSet.close();
        statement.close();
        sqlConnection.close();
    }

    @Test
    void givenConfigurationFileWithSslEnabled_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        final ClassLoader classLoader = this.getClass().getClassLoader();
        final URL confTestUrl = classLoader.getResource("test_application_with_ssl.conf");
        if (confTestUrl == null) {
            fail("Unable to find test_application_with_ssl.conf");
        }

        // Update the truststore path in the configuration file and store the modified file in a temporary location.
        String content = new String(Files.readAllBytes(Paths.get(confTestUrl.toURI())));
        content = content.replaceAll("\\$TRUSTSTORE_PATH",
            Objects.requireNonNull(classLoader.getResource("cassandra.truststore")).getPath());
        final Path updatedConfTestPath = Files.createTempFile("test_application_with_ssl_", ".conf");
        Files.write(updatedConfTestPath, content.getBytes(StandardCharsets.UTF_8));

        initConnection(KEYSPACE, "configfile=" + updatedConfTestPath);
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        assertTrue(sqlConnection.getSession().getContext().getSslEngineFactory().isPresent());

        final Statement statement = sqlConnection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT * FROM system.local");
        assertNotNull(resultSet);
        resultSet.close();
        statement.close();
        sqlConnection.close();
    }

    @Test
    void givenSslEngineFactory_whenConfigureSsl_addGivenSslEngineFactoryToSessionBuilder() throws Exception {
        final SessionHolder sessionHolder = new SessionHolder(Collections.singletonMap(URL_KEY,
            buildJdbcUrl(cassandraContainer.getContactPoint().getHostName(),
                cassandraContainer.getContactPoint().getPort(), KEYSPACE, "localdatacenter=datacenter1")), null);
        final CqlSessionBuilder cqlSessionBuilder = spy(new CqlSessionBuilder());
        sessionHolder.configureSslEngineFactory(cqlSessionBuilder,
            "com.ing.data.cassandra.jdbc.utils.FakeSslEngineFactory");
        verify(cqlSessionBuilder).withSslEngineFactory(any(FakeSslEngineFactory.class));
    }

    @Test
    void givenSessionToConnect() throws SQLException {
        final CqlSession session = CqlSession.builder()
            .addContactPoint(cassandraContainer.getContactPoint())
            .withLocalDatacenter("datacenter1")
            .build();

        final CassandraConnection jdbcConnection =
            new CassandraConnection(session, KEYSPACE, ConsistencyLevel.ALL, false, null);
        final ResultSet resultSet = jdbcConnection.createStatement()
            .executeQuery("SELECT release_version FROM system.local");
        assertNotNull(resultSet.getString("release_version"));
        assertEquals("embedded_test_cluster", jdbcConnection.getCatalog());
    }

    @Test
    void givenSessionToConnect_andLiquibaseCompliance() throws SQLException {
        final CqlSession session = CqlSession.builder()
                .addContactPoint(cassandraContainer.getContactPoint())
                .withLocalDatacenter("datacenter1")
                .build();

        final Liquibase liquibaseMode = new Liquibase();
        final CassandraConnection jdbcConnection =
            new CassandraConnection(session, KEYSPACE, ConsistencyLevel.ALL, false, liquibaseMode);
        liquibaseMode.setConnection(jdbcConnection);
        final ResultSet resultSet = jdbcConnection.createStatement()
            .executeQuery("SELECT release_version FROM system.local");
        assertNotNull(resultSet.getString("release_version"));
        assertEquals(KEYSPACE, jdbcConnection.getCatalog());
    }

    @Test
    void givenConnection_whenGetMetaData_getExpectedResultSet() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getMetaData());

        final DatabaseMetaData dbMetadata = sqlConnection.getMetaData();
        LOG.debug("====================================================");
        LOG.debug("Connection Metadata");
        LOG.debug("====================================================");
        LOG.debug("Driver name: {}", dbMetadata.getDriverName());
        LOG.debug("Driver version: {}", dbMetadata.getDriverVersion());
        LOG.debug("DB name: {}", dbMetadata.getDatabaseProductName());
        LOG.debug("DB version: {}", dbMetadata.getDatabaseProductVersion());
        LOG.debug("JDBC version: {}.{}", dbMetadata.getJDBCMajorVersion(), dbMetadata.getJDBCMinorVersion());
        LOG.debug("====================================================");

        assertEquals("Cassandra JDBC Driver", dbMetadata.getDriverName());
        assertNotEquals(0, dbMetadata.getDriverMajorVersion());
        assertNotEquals(0, dbMetadata.getDriverMinorVersion());
        assertEquals(4, dbMetadata.getJDBCMajorVersion());
        assertEquals(0, dbMetadata.getJDBCMinorVersion());
        assertEquals("Cassandra", dbMetadata.getDatabaseProductName());
        assertThat(dbMetadata.getDriverVersion(), Matchers.matchesPattern("\\d.\\d+.\\d+"));
        assertThat(dbMetadata.getDatabaseProductVersion(), Matchers.matchesPattern("\\d.\\d+.\\d+"));
        sqlConnection.close();
    }

    @Test
    void givenCassandraConnection_whenUnwrap_returnUnwrappedConnection() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
        assertNotNull(sqlConnection.unwrap(Connection.class));
    }

    @Test
    void givenCassandraConnection_whenUnwrapToInvalidInterface_throwException() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
        assertThrows(SQLException.class, () -> sqlConnection.unwrap(this.getClass()));
    }

    @Test
    void givenCassandraConnectionAndNegativeTimeout_whenIsValid_throwException() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
        final SQLTimeoutException sqlTimeoutException = assertThrows(SQLTimeoutException.class,
            () -> sqlConnection.isValid(-1));
        assertEquals(BAD_TIMEOUT, sqlTimeoutException.getMessage());
    }

    @Test
    void givenOpenCassandraConnection_whenIsValid_returnTrue() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
        assertTrue(sqlConnection.isValid(0));
    }

    @Test
    void givenClosedCassandraConnection_whenIsValid_returnFalse() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
        sqlConnection.close();
        assertFalse(sqlConnection.isValid(0));
    }

    @Test
    void givenCassandraConnectionAndClosedSession_whenIsValid_returnFalse() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
        sqlConnection.getSession().close();
        assertFalse(sqlConnection.isClosed());
        assertFalse(sqlConnection.isValid(0));
    }

    @Test
    void givenOpenCassandraConnectionAndTimedOutQuery_whenIsValid_returnFalse() throws Exception {
        final CqlSession session = CqlSession.builder()
            .addContactPoint(cassandraContainer.getContactPoint())
            .withLocalDatacenter("datacenter1")
            .build();

        final CassandraConnection jdbcConnection =
            spy(new CassandraConnection(session, KEYSPACE, ConsistencyLevel.ALL, false, null));
        final CassandraStatement mockStmt = mock(CassandraStatement.class);
        when(mockStmt.execute(anyString())).then(invocationOnMock -> {
            // We test isValid() with a timeout of 1 second, so wait more than 1 second to simulate a query timeout.
            Thread.sleep(1500);
            return true;
        });
        when(jdbcConnection.createStatement()).thenReturn(mockStmt);
        assertFalse(jdbcConnection.isValid(1));
    }

    @Test
    void givenCassandraConnection_whenSetQueryTimeout_updateRequestTimeoutAsExpected() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1", "requesttimeout=2000");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        assertNotNull(sqlConnection.getSession().getContext().getConfig());
        assertNotNull(sqlConnection.getSession().getContext().getConfig().getDefaultProfile());
        assertEquals(Duration.ofSeconds(2), sqlConnection.getSession().getContext().getConfig()
            .getDefaultProfile().getDuration(DefaultDriverOption.REQUEST_TIMEOUT));

        final Statement statement = sqlConnection.createStatement();
        assertEquals(2, statement.getQueryTimeout());
        statement.setQueryTimeout(1);
        assertEquals(1, statement.getQueryTimeout());
    }

}
