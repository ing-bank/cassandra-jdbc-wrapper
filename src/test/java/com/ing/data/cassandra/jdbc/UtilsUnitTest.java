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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverOption;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static com.ing.data.cassandra.jdbc.utils.DriverUtil.getDriverProperty;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.parseVersion;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_KEYSPACE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.HOST_IN_URL;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.HOST_REQUIRED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.SECURECONENCTBUNDLE_REQUIRED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.URI_IS_SIMPLE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.DEFAULT_PORT;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.PROTOCOL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CLOUD_SECURE_CONNECT_BUNDLE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONNECTION_RETRIES;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONSISTENCY_LEVEL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CQL_VERSION;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_DATABASE_NAME;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_DEBUG;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_LOAD_BALANCING_POLICY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_LOCAL_DATACENTER;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_PASSWORD;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_PORT_NUMBER;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_RECONNECT_POLICY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_REQUEST_TIMEOUT;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_RETRY_POLICY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_SERVER_NAME;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_USER;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.createSubName;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.parseReconnectionPolicy;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.parseURL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UtilsUnitTest {

    static Stream<Arguments> buildUrlParsingTestCases() {
        return Stream.of(
            Arguments.of("jdbc:cassandra://localhost:9042/astra?secureconnectbundle=/path/to/location/filename.extn&user=user1&password=password1",
                new HashMap<String, String>() {{
                    put(TAG_SERVER_NAME, "localhost");
                    put(TAG_PORT_NUMBER, "9042");
                    put(TAG_DATABASE_NAME, "astra");
                    put(TAG_CLOUD_SECURE_CONNECT_BUNDLE, "/path/to/location/filename.extn");
                    put(TAG_USER, "user1");
                    put(TAG_PASSWORD, "password1");
                }}),
            Arguments.of("jdbc:cassandra:dbaas:///astra?secureconnectbundle=/path/to/location/filename.extn&user=user1&password=password1",
                new HashMap<String, String>() {{
                    put(TAG_SERVER_NAME, null);
                    put(TAG_PORT_NUMBER, String.valueOf(DEFAULT_PORT));
                    put(TAG_DATABASE_NAME, "astra");
                    put(TAG_CLOUD_SECURE_CONNECT_BUNDLE, "/path/to/location/filename.extn");
                    put(TAG_USER, "user1");
                    put(TAG_PASSWORD, "password1");
                }}),
            Arguments.of("jdbc:cassandra://localhost:9042/Keyspace1?version=3.0.0&consistency=QUORUM",
                new HashMap<String, String>() {{
                    put(TAG_SERVER_NAME, "localhost");
                    put(TAG_PORT_NUMBER, "9042");
                    put(TAG_DATABASE_NAME, "Keyspace1");
                    put(TAG_CQL_VERSION, "3.0.0");
                    put(TAG_CONSISTENCY_LEVEL, "QUORUM");
                }}),
            Arguments.of("jdbc:cassandra://localhost/Keyspace1?consistency=QUORUM",
                new HashMap<String, String>() {{
                    put(TAG_SERVER_NAME, "localhost");
                    put(TAG_PORT_NUMBER, "9042");
                    put(TAG_DATABASE_NAME, "Keyspace1");
                    put(TAG_CQL_VERSION, null);
                    put(TAG_CONSISTENCY_LEVEL, "QUORUM");
                }}),
            Arguments.of("jdbc:cassandra://localhost/Keyspace1?version=2.0.0",
                new HashMap<String, String>() {{
                    put(TAG_SERVER_NAME, "localhost");
                    put(TAG_PORT_NUMBER, "9042");
                    put(TAG_DATABASE_NAME, "Keyspace1");
                    put(TAG_CQL_VERSION, "2.0.0");
                    put(TAG_CONSISTENCY_LEVEL, null);
                }}),
            Arguments.of("jdbc:cassandra://localhost",
                new HashMap<String, String>() {{
                    put(TAG_SERVER_NAME, "localhost");
                    put(TAG_PORT_NUMBER, "9042");
                    put(TAG_DATABASE_NAME, null);
                    put(TAG_CQL_VERSION, null);
                    put(TAG_CONSISTENCY_LEVEL, null);
                }}),
            Arguments.of("jdbc:cassandra://localhost/Keyspace1?localdatacenter=DC1",
                new HashMap<String, String>() {{
                    put(TAG_SERVER_NAME, "localhost");
                    put(TAG_PORT_NUMBER, "9042");
                    put(TAG_DATABASE_NAME, "Keyspace1");
                    put(TAG_LOCAL_DATACENTER, "DC1");
                }}),
            Arguments.of("jdbc:cassandra://localhost/Keyspace1?localdatacenter=DC1&debug=true"
                    + "&retries=5&requesttimeout=3000&loadbalancing=com.company.package.CustomLBPolicy"
                    + "&retry=com.company.package.CustomRetryPolicy&reconnection=ConstantReconnectionPolicy()",
                new HashMap<String, String>() {{
                    put(TAG_SERVER_NAME, "localhost");
                    put(TAG_PORT_NUMBER, "9042");
                    put(TAG_DATABASE_NAME, "Keyspace1");
                    put(TAG_LOCAL_DATACENTER, "DC1");
                    put(TAG_DEBUG, "true");
                    put(TAG_CONNECTION_RETRIES, "5");
                    put(TAG_LOAD_BALANCING_POLICY, "com.company.package.CustomLBPolicy");
                    put(TAG_RETRY_POLICY, "com.company.package.CustomRetryPolicy");
                    put(TAG_RECONNECT_POLICY, "ConstantReconnectionPolicy()");
                    put(TAG_REQUEST_TIMEOUT, "3000");
                }})
        );
    }

    @ParameterizedTest
    @MethodSource("buildUrlParsingTestCases")
    void givenJdbcUrl_whenParseUrl_returnExpectedProperties(final String jdbcUrl,
                                                            final Map<String, String> expectedProperties)
        throws SQLException {
        final Properties result = parseURL(jdbcUrl);
        expectedProperties.forEach((key, value) -> assertEquals(value, result.getProperty(key)));
    }

    static Stream<Arguments> buildReconnectionPolicyParsingTestCases() {
        return Stream.of(
            Arguments.of("ExponentialReconnectionPolicy()",
                new HashMap<DriverOption, Object>() {{
                    put(DefaultDriverOption.RECONNECTION_POLICY_CLASS,
                        "com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy");
                    put(DefaultDriverOption.RECONNECTION_BASE_DELAY, null);
                    put(DefaultDriverOption.RECONNECTION_MAX_DELAY, null);
                }}),
            Arguments.of("ExponentialReconnectionPolicy((long)2,(long)120)",
                new HashMap<DriverOption, Object>() {{
                    put(DefaultDriverOption.RECONNECTION_POLICY_CLASS,
                        "com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy");
                    put(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofSeconds(2L));
                    put(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofSeconds(120L));
                }}),
            Arguments.of("ConstantReconnectionPolicy()",
                new HashMap<DriverOption, Object>() {{
                    put(DefaultDriverOption.RECONNECTION_POLICY_CLASS,
                        "com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy");
                    put(DefaultDriverOption.RECONNECTION_BASE_DELAY, null);
                    put(DefaultDriverOption.RECONNECTION_MAX_DELAY, null);
                }}),
            Arguments.of("ConstantReconnectionPolicy((long)25)",
                new HashMap<DriverOption, Object>() {{
                    put(DefaultDriverOption.RECONNECTION_POLICY_CLASS,
                        "com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy");
                    put(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofSeconds(25L));
                    put(DefaultDriverOption.RECONNECTION_MAX_DELAY, null);
                }})
        );
    }

    @ParameterizedTest
    @MethodSource("buildReconnectionPolicyParsingTestCases")
    void givenReconnectionPolicyString_whenParsePolicy_returnExpectedOptions(
        final String policyString, final Map<DriverOption, Object> expectedPolicy) {
        final Map<DriverOption, Object> policyOptions = parseReconnectionPolicy(policyString);
        assertNotNull(policyOptions);
        expectedPolicy.forEach((key, value) -> assertEquals(value, policyOptions.get(key)));
    }

    @Test
    void testCreateSubName() throws Exception {
        final String jdbcUrl = "jdbc:cassandra://localhost:9042/Keyspace1?consistency=QUORUM&version=3.0.0";
        final Properties props = parseURL(jdbcUrl);
        final String result = createSubName(props);
        assertEquals(jdbcUrl, PROTOCOL + result);
    }

    @Test
    void testCreateSubNameWithoutParams() throws Exception {
        final String jdbcUrl = "jdbc:cassandra://localhost:9042/Keyspace1";
        final Properties props = parseURL(jdbcUrl);
        final String result = createSubName(props);
        assertEquals(jdbcUrl, PROTOCOL + result);
    }

    @Test
    void testInvalidJdbcUrl() {
        assertThrows(SQLSyntaxErrorException.class, () -> parseURL("jdbc:cassandra/bad%uri"));
    }

    @Test
    void testNullHost() {
        final SQLNonTransientConnectionException exception = assertThrows(SQLNonTransientConnectionException.class,
            () -> parseURL("jdbc:cassandra:"));
        assertEquals(HOST_IN_URL, exception.getMessage());
    }

    @Test
    void testInvalidKeyspaceName() {
        final String invalidKeyspaceName = "bad-keyspace";
        final SQLNonTransientConnectionException exception = assertThrows(SQLNonTransientConnectionException.class,
            () -> parseURL("jdbc:cassandra://hostname:9042/" + invalidKeyspaceName));
        assertEquals(String.format(BAD_KEYSPACE, invalidKeyspaceName), exception.getMessage());
    }

    @Test
    void testNotNullUserInfo() {
        final SQLNonTransientConnectionException exception = assertThrows(SQLNonTransientConnectionException.class,
            () -> parseURL("jdbc:cassandra://john_doe@hostname:9042/validKeyspace"));
        assertEquals(URI_IS_SIMPLE, exception.getMessage());
    }

    @Test
    void testCreateSubNameWithoutHost() throws Exception {
        final String jdbcUrl = "jdbc:cassandra://localhost:9042/Keyspace1";
        final Properties props = parseURL(jdbcUrl);
        props.remove(TAG_SERVER_NAME);
        final SQLNonTransientConnectionException exception = assertThrows(SQLNonTransientConnectionException.class,
            () -> createSubName(props));
        assertEquals(HOST_REQUIRED, exception.getMessage());
    }

    @Test
    void testCreateSubNameWithInvalidPortNumber() throws Exception {
        final String jdbcUrl = "jdbc:cassandra://localhost/Keyspace1";
        final Properties props = parseURL(jdbcUrl);
        props.put(TAG_PORT_NUMBER, "-9042");
        assertThrows(SQLNonTransientConnectionException.class, () -> createSubName(props));
    }

    @ParameterizedTest
    @ValueSource(strings = {"jdbc:cassandra:dbaas:///astra", "jdbc:cassandra:dbaas:///astra?user=User1"})
    void testMissingSecureConnectBundleOnDbaasConenctionString(final String jdbcUrl) {
        final SQLNonTransientConnectionException exception = assertThrows(SQLNonTransientConnectionException.class,
            () -> parseURL(jdbcUrl));
        assertEquals(SECURECONENCTBUNDLE_REQUIRED, exception.getMessage());
    }

    @Test
    void testGetDriverProperty() {
        assertEquals(StringUtils.EMPTY, getDriverProperty("invalidProperty"));
        assertNotNull(getDriverProperty("driver.name"));
    }

    @Test
    void testParseVersion() {
        assertEquals(0, parseVersion(StringUtils.EMPTY, 0));
        assertEquals(0, parseVersion("1.0.0", 3));
        assertEquals(0, parseVersion("1.0.0", -1));
        assertEquals(1, parseVersion("1.2.3", 0));
        assertEquals(2, parseVersion("1.2.3", 1));
        assertEquals(3, parseVersion("1.2.3", 2));
        assertEquals(0, parseVersion("1.a", 1));
    }
}
