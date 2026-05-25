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

package com.ing.data.cassandra.jdbc.testing;

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
import com.ing.data.cassandra.jdbc.CassandraConnection;
import com.ing.data.cassandra.jdbc.utils.ErrorConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.time.Duration;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.Optional;

import static com.ing.data.cassandra.jdbc.utils.ConversionsUtil.milliOfDayToLocalTime;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;
import static java.time.temporal.ChronoField.MILLI_OF_DAY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Utilities methods for assertions specific to the JDBC driver tests.
 */
public final class AssertionsUtils {

    /**
     * Assert that execution of the supplied executable throws a {@link SQLFeatureNotSupportedException} or a subtype
     * thereof, with the message {@value ErrorConstants#NOT_SUPPORTED }.
     * If no exception is thrown, or if an exception of a different type is thrown, this method will fail.
     *
     * @param executable The verified executable.
     * @see Assertions#assertThrows(Class, Executable)
     */
    public static void assertNotImplemented(final Executable executable) {
        final SQLFeatureNotSupportedException sqlEx = assertThrows(SQLFeatureNotSupportedException.class, executable);
        assertEquals(NOT_SUPPORTED, sqlEx.getMessage());
    }

    /**
     * Assert that the specified {@link java.sql.Time} is equal to the expected time value in milliseconds since
     * January 1, 1970, 00:00:00 GMT (i.e. in milliseconds within a day).
     * <p>
     *     Equality of timestamp in {@link ChronoField#MILLI_OF_DAY} and string representation of the corresponding
     *     {@link LocalTime} are tested.
     * </p>
     *
     * @param expected The expected time value.
     * @param actual   The actual SQL time value to test.
     */
    public static void assertTimeEquals(final long expected, final Time actual) {
        assertNotNull(actual);
        assertEquals(expected, actual.getTime());
        assertEquals(milliOfDayToLocalTime(expected).toString(), milliOfDayToLocalTime(actual.getTime()).toString());
    }

    /**
     * Assert that the specified {@link java.sql.Time} is equal to the expected {@link LocalTime} value.
     * <p>
     *     Equality of timestamp in {@link ChronoField#MILLI_OF_DAY} and string representation of the corresponding
     *     {@link LocalTime} are tested.
     * </p>
     *
     * @param expected The expected {@link LocalTime} value.
     * @param actual   The actual SQL time value to test.
     */
    public static void assertTimeEquals(final LocalTime expected, final Time actual) {
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.getLong(MILLI_OF_DAY), actual.getTime());
        assertEquals(expected.toString(), milliOfDayToLocalTime(actual.getTime()).toString());
    }

    public static void assertConnectionHasExpectedConfig(final CassandraConnection connection,
                                                         final String expectedKeyspace) {
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
        final ConsistencyLevel serialConsistencyLevel = connection.getSerialConsistencyLevel();
        assertNotNull(serialConsistencyLevel);
        assertEquals(ConsistencyLevel.LOCAL_SERIAL, serialConsistencyLevel);

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
        assertEquals(expectedKeyspace, connection.getSession().getKeyspace().get().asCql(true));
    }

}
