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

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.ing.data.cassandra.jdbc.utils.FakeLoadBalancingPolicy;
import com.ing.data.cassandra.jdbc.utils.FakeReconnectionPolicy;
import com.ing.data.cassandra.jdbc.utils.FakeRetryPolicy;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConnectionUnitTest extends UsingEmbeddedCassandraServerTest {

    private static final String KEYSPACE = "system";

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

}
