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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import com.ing.data.cassandra.jdbc.utils.AnotherFakeLoadBalancingPolicy;
import com.ing.data.cassandra.jdbc.utils.AnotherFakeRetryPolicy;
import com.ing.data.cassandra.jdbc.utils.FakeLoadBalancingPolicy;
import com.ing.data.cassandra.jdbc.utils.FakeReconnectionPolicy;
import com.ing.data.cassandra.jdbc.utils.FakeRetryPolicy;
import com.ing.data.cassandra.jdbc.utils.FakeSslEngineFactory;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import static com.ing.data.cassandra.jdbc.SessionHolder.URL_KEY;
import static com.ing.data.cassandra.jdbc.Utils.JSSE_KEYSTORE_PASSWORD_PROPERTY;
import static com.ing.data.cassandra.jdbc.Utils.JSSE_KEYSTORE_PROPERTY;
import static com.ing.data.cassandra.jdbc.Utils.JSSE_TRUSTSTORE_PASSWORD_PROPERTY;
import static com.ing.data.cassandra.jdbc.Utils.JSSE_TRUSTSTORE_PROPERTY;
import static com.ing.data.cassandra.jdbc.Utils.SSL_CONFIG_FAILED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class ConnectionUnitTest extends UsingEmbeddedCassandraServerTest {

    private static final String KEYSPACE = "system";

    @Test
    void givenInvalidConfigurationFile_whenGetConnection_createConnectionIgnoringConfigFile() throws Exception {
        initConnection(KEYSPACE, "configfile=wrong_application.conf", "consistency=LOCAL_QUORUM");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getDefaultConsistencyLevel());
        final ConsistencyLevel consistencyLevel = sqlConnection.getDefaultConsistencyLevel();
        assertNotNull(consistencyLevel);
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, consistencyLevel);
        sqlConnection.close();
    }

    @Test
    void givenValidConfigurationFile_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        final URL confTestUrl = this.getClass().getClassLoader().getResource("test_application.conf");
        if (confTestUrl == null) {
            fail("Unable to find test_application.conf");
        }
        initConnection(KEYSPACE, "configfile=" + confTestUrl.getPath(), "localdatacenter=DC2",
            "user=aTestUser", "password=aTestPassword",
            "loadbalancing=com.ing.data.cassandra.jdbc.utils.FakeLoadBalancingPolicy",
            "retry=com.ing.data.cassandra.jdbc.utils.FakeRetryPolicy",
            "reconnection=com.ing.data.cassandra.jdbc.utils.FakeReconnectionPolicy()",
            "sslenginefactory=com.ing.data.cassandra.jdbc.utils.FakeSslEngineFactory");
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        assertNotNull(sqlConnection.getDefaultConsistencyLevel());
        final ConsistencyLevel consistencyLevel = sqlConnection.getDefaultConsistencyLevel();
        assertNotNull(consistencyLevel);
        assertEquals(ConsistencyLevel.TWO, consistencyLevel);

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
        assertEquals(reconnectionPolicy.newControlConnectionSchedule(false).nextDelay(), Duration.ofSeconds(10));

        // Check the not overridden values.
        assertTrue(sqlConnection.getSession().getKeyspace().isPresent());
        assertEquals(KEYSPACE, sqlConnection.getSession().getKeyspace().get().asCql(true));
        sqlConnection.close();
    }

    @Test
    void givenNoLoadBalancingPolicy_whenGetConnection_createConnectionWithExpectedConfig() throws Exception {
        initConnection(KEYSPACE, StringUtils.EMPTY);
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
        initConnection(KEYSPACE, StringUtils.EMPTY);
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
        initConnection(KEYSPACE, "retry=com.ing.data.cassandra.jdbc.utils.FakeRetryPolicy");
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
        initConnection(KEYSPACE, "reconnection=ConstantReconnectionPolicy((long)10)");
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
        initConnection(KEYSPACE, "reconnection=ExponentialReconnectionPolicy((long)10,(long)100)");
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
        initConnection(KEYSPACE, "reconnection=com.ing.data.cassandra.jdbc.utils.FakeReconnectionPolicy()");
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
        initConnection(KEYSPACE, "reconnection=ExponentialReconnectionPolicy((int)100)");
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
        initConnection(KEYSPACE, "enablessl=false");
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
     * The resources 'test_keystore.jks' and 'test_truststore.jks' are provided for testing purpose only. They contain
     * self-signed certificate to not use in a production context.
     */

    @Test
    void givenEnabledSslWithJsse_whenConfigureSsl_addDefaultSslEngineFactoryToSessionBuilder() throws Exception {
        final ClassLoader classLoader = this.getClass().getClassLoader();
        System.setProperty(JSSE_TRUSTSTORE_PROPERTY,
            Objects.requireNonNull(classLoader.getResource("test_truststore.jks")).getPath());
        System.setProperty(JSSE_TRUSTSTORE_PASSWORD_PROPERTY, "changeit");
        System.setProperty(JSSE_KEYSTORE_PROPERTY,
            Objects.requireNonNull(classLoader.getResource("test_keystore.jks")).getPath());
        System.setProperty(JSSE_KEYSTORE_PASSWORD_PROPERTY, "changeit");

        // TODO: re-write this test with a connection to an embedded Cassandra server supporting SSL.
        final SessionHolder sessionHolder = new SessionHolder(Collections.singletonMap(URL_KEY,
            buildJdbcUrl(BuildCassandraServer.HOST, BuildCassandraServer.PORT, KEYSPACE)), null);
        final CqlSessionBuilder cqlSessionBuilder = spy(new CqlSessionBuilder());
        sessionHolder.configureDefaultSslEngineFactory(cqlSessionBuilder, DriverConfigLoader.programmaticBuilder());
        verify(cqlSessionBuilder).withSslEngineFactory(any(DefaultSslEngineFactory.class));
    }

    @Test
    void givenSslEngineFactory_whenConfigureSsl_addGivenSslEngineFactoryToSessionBuilder() throws Exception {
        // TODO: re-write this test with a connection to an embedded Cassandra server supporting SSL.
        final SessionHolder sessionHolder = new SessionHolder(Collections.singletonMap(URL_KEY,
            buildJdbcUrl(BuildCassandraServer.HOST, BuildCassandraServer.PORT, KEYSPACE)), null);
        final CqlSessionBuilder cqlSessionBuilder = spy(new CqlSessionBuilder());
        sessionHolder.configureSslEngineFactory(cqlSessionBuilder,
            "com.ing.data.cassandra.jdbc.utils.FakeSslEngineFactory");
        verify(cqlSessionBuilder).withSslEngineFactory(any(FakeSslEngineFactory.class));
    }

}
