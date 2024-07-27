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
import com.ing.data.cassandra.jdbc.metadata.BasicVersionedMetadata;
import com.ing.data.cassandra.jdbc.utils.ContactPoint;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.semver4j.Semver;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static com.ing.data.cassandra.jdbc.utils.DriverUtil.CASSANDRA_4;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.CASSANDRA_5;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.buildMetadataList;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.existsInDatabaseVersion;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.getDriverProperty;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.safeParseVersion;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_KEYSPACE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.HOST_IN_URL;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.HOST_REQUIRED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.INVALID_CONTACT_POINT;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.SECURECONENCTBUNDLE_REQUIRED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.URI_IS_SIMPLE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.PROTOCOL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CLOUD_SECURE_CONNECT_BUNDLE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONNECTION_RETRIES;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONSISTENCY_LEVEL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONTACT_POINTS;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_DATABASE_NAME;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_DEBUG;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_LOAD_BALANCING_POLICY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_LOCAL_DATACENTER;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_PASSWORD;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_RECONNECT_POLICY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_REQUEST_TIMEOUT;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_RETRY_POLICY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_USER;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.createSubName;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.parseReconnectionPolicy;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.parseURL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UtilsUnitTest {

    static Stream<Arguments> buildUrlParsingTestCases() {
        return Stream.of(
            Arguments.of("jdbc:cassandra://localhost:9042/astra?secureconnectbundle=/path/to/location/filename.extn&user=user1&password=password1",
                new HashMap<String, Object>() {{
                    put(TAG_CONTACT_POINTS, Collections.singletonList(ContactPoint.of("localhost", 9042)));
                    put(TAG_DATABASE_NAME, "astra");
                    put(TAG_CLOUD_SECURE_CONNECT_BUNDLE, "/path/to/location/filename.extn");
                    put(TAG_USER, "user1");
                    put(TAG_PASSWORD, "password1");
                }}),
            Arguments.of("jdbc:cassandra:dbaas:///astra?secureconnectbundle=/path/to/location/filename.extn&user=user1&password=password1",
                new HashMap<String, Object>() {{
                    put(TAG_CONTACT_POINTS, null);
                    put(TAG_DATABASE_NAME, "astra");
                    put(TAG_CLOUD_SECURE_CONNECT_BUNDLE, "/path/to/location/filename.extn");
                    put(TAG_USER, "user1");
                    put(TAG_PASSWORD, "password1");
                }}),
            Arguments.of("jdbc:cassandra://localhost:9042/Keyspace1?consistency=QUORUM",
                new HashMap<String, Object>() {{
                    put(TAG_CONTACT_POINTS, Collections.singletonList(ContactPoint.of("localhost", 9042)));
                    put(TAG_DATABASE_NAME, "Keyspace1");
                    put(TAG_CONSISTENCY_LEVEL, "QUORUM");
                }}),
            Arguments.of("jdbc:cassandra://localhost/Keyspace1?consistency=QUORUM",
                new HashMap<String, Object>() {{
                    put(TAG_CONTACT_POINTS, Collections.singletonList(ContactPoint.of("localhost", 9042)));
                    put(TAG_DATABASE_NAME, "Keyspace1");
                    put(TAG_CONSISTENCY_LEVEL, "QUORUM");
                }}),
            Arguments.of("jdbc:cassandra://localhost/Keyspace1",
                new HashMap<String, Object>() {{
                    put(TAG_CONTACT_POINTS, Collections.singletonList(ContactPoint.of("localhost", 9042)));
                    put(TAG_DATABASE_NAME, "Keyspace1");
                    put(TAG_CONSISTENCY_LEVEL, null);
                }}),
            Arguments.of("jdbc:cassandra://localhost",
                new HashMap<String, Object>() {{
                    put(TAG_CONTACT_POINTS, Collections.singletonList(ContactPoint.of("localhost", 9042)));
                    put(TAG_DATABASE_NAME, null);
                    put(TAG_CONSISTENCY_LEVEL, null);
                }}),
            Arguments.of("jdbc:cassandra://localhost/Keyspace1?localdatacenter=DC1",
                new HashMap<String, Object>() {{
                    put(TAG_CONTACT_POINTS, Collections.singletonList(ContactPoint.of("localhost", 9042)));
                    put(TAG_DATABASE_NAME, "Keyspace1");
                    put(TAG_LOCAL_DATACENTER, "DC1");
                }}),
            Arguments.of("jdbc:cassandra://127.0.0.1/Keyspace1?localdatacenter=DC1&debug=true"
                    + "&retries=5&requesttimeout=3000&loadbalancing=com.company.package.CustomLBPolicy"
                    + "&retry=com.company.package.CustomRetryPolicy&reconnection=ConstantReconnectionPolicy()",
                new HashMap<String, Object>() {{
                    put(TAG_CONTACT_POINTS, Collections.singletonList(ContactPoint.of("127.0.0.1", 9042)));
                    put(TAG_DATABASE_NAME, "Keyspace1");
                    put(TAG_LOCAL_DATACENTER, "DC1");
                    put(TAG_DEBUG, "true");
                    put(TAG_CONNECTION_RETRIES, "5");
                    put(TAG_LOAD_BALANCING_POLICY, "com.company.package.CustomLBPolicy");
                    put(TAG_RETRY_POLICY, "com.company.package.CustomRetryPolicy");
                    put(TAG_RECONNECT_POLICY, "ConstantReconnectionPolicy()");
                    put(TAG_REQUEST_TIMEOUT, "3000");
                }}),
            Arguments.of("jdbc:cassandra://host1--host2",
                new HashMap<String, Object>() {{
                    put(TAG_CONTACT_POINTS, Arrays.asList(ContactPoint.of("host1", 9042),
                        ContactPoint.of("host2", 9042)));
                }}),
            Arguments.of("jdbc:cassandra://host1--host2:9043",
                new HashMap<String, Object>() {{
                    put(TAG_CONTACT_POINTS, Arrays.asList(ContactPoint.of("host1", 9043),
                        ContactPoint.of("host2", 9043)));
                }}),
            Arguments.of("jdbc:cassandra://host1:9042--host2:9043",
                new HashMap<String, Object>() {{
                    put(TAG_CONTACT_POINTS, Arrays.asList(ContactPoint.of("host1", 9042),
                        ContactPoint.of("host2", 9043)));
                }}),
            Arguments.of("jdbc:cassandra://host1:9042--host2--host3:9043",
                new HashMap<String, Object>() {{
                    put(TAG_CONTACT_POINTS, Arrays.asList(ContactPoint.of("host1", 9042),
                        ContactPoint.of("host2", 9043), ContactPoint.of("host3", 9043)));
                }})
        );
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("buildUrlParsingTestCases")
    void givenJdbcUrl_whenParseUrl_returnExpectedProperties(final String jdbcUrl,
                                                            final Map<String, Object> expectedProperties)
        throws SQLException {
        final Properties result = parseURL(jdbcUrl);
        expectedProperties.forEach((key, value) -> {
            if (TAG_CONTACT_POINTS.equals(key) && value instanceof List) {
                final List<ContactPoint> expectedContactPoints = (List<ContactPoint>) value;
                assertThat((List<ContactPoint>) result.get(key),
                    containsInAnyOrder(expectedContactPoints.toArray(new ContactPoint[0])));
            } else {
                assertEquals(value, result.getProperty(key));
            }
        });
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
        final String jdbcUrl = "jdbc:cassandra://localhost:9042/Keyspace1?consistency=QUORUM";
        final Properties props = parseURL(jdbcUrl);
        final String result = createSubName(props);
        assertEquals(jdbcUrl, PROTOCOL + result);
    }

    @Test
    void testCreateSubNameWithMultipleContactPoints() throws Exception {
        final String jdbcUrl = "jdbc:cassandra://host1:9042--host2--host3:9043/Keyspace1?consistency=QUORUM";
        final Properties props = parseURL(jdbcUrl);
        final String result = createSubName(props);
        assertEquals("jdbc:cassandra://host1:9042--host2:9043--host3:9043/Keyspace1?consistency=QUORUM",
            PROTOCOL + result);
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
    void testHostIsIPv6() {
        assertDoesNotThrow(() -> {
            parseURL("jdbc:cassandra://[0000:1111:2222:3333:4444:5555:aaaa:ffff]:9042/validKeyspace");
            parseURL("jdbc:cassandra://[0000:1111:2222:3333:::ffff]:9043--[0123::456b:789c:ffff]:9042/validKeyspace");
            parseURL("jdbc:cassandra://[0000:1111:2222:3333:aaaa::ffff]--127.0.0.1--cassandra-host:9042/validKeyspace");
            parseURL("jdbc:cassandra://[0000::ffff]:9042/validKeyspace");
        });
    }

    @Test
    void testInvalidPort() {
        final SQLNonTransientConnectionException exception = assertThrows(SQLNonTransientConnectionException.class,
            () -> parseURL("jdbc:cassandra://localhost:badPort"));
        assertEquals(String.format(INVALID_CONTACT_POINT, "localhost:badPort"), exception.getMessage());
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
    void testCreateSubNameWithoutContactPoints() throws Exception {
        final String jdbcUrl = "jdbc:cassandra://localhost:9042/Keyspace1";
        final Properties props = parseURL(jdbcUrl);
        props.remove(TAG_CONTACT_POINTS);
        final SQLNonTransientConnectionException exception = assertThrows(SQLNonTransientConnectionException.class,
            () -> createSubName(props));
        assertEquals(HOST_REQUIRED, exception.getMessage());
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
    void testSafeParseVersion() {
        assertEquals(Semver.ZERO, safeParseVersion(StringUtils.EMPTY));
        assertEquals(Semver.ZERO, safeParseVersion("alpha"));
        assertEquals(Semver.parse("1.0.0"), safeParseVersion("1"));
        assertEquals(Semver.parse("1.0.0"), safeParseVersion("1.0"));
        assertEquals(Semver.parse("1.2.3"), safeParseVersion("1.2.3"));
    }

    @Test
    void testExistsInDatabaseVersion() {
        assertTrue(existsInDatabaseVersion(CASSANDRA_4, new BasicVersionedMetadata("TEST")));
        assertTrue(existsInDatabaseVersion(CASSANDRA_5, new BasicVersionedMetadata("TEST", CASSANDRA_4)));
        assertFalse(existsInDatabaseVersion(CASSANDRA_5, new BasicVersionedMetadata("TEST", null, CASSANDRA_5)));
        assertFalse(existsInDatabaseVersion(CASSANDRA_4, new BasicVersionedMetadata("TEST", CASSANDRA_5)));
    }

    @Test
    void testBuildMetadataList() {
        assertEquals("a,b,d", buildMetadataList(Arrays.asList(
            new BasicVersionedMetadata("a"),
            new BasicVersionedMetadata("d", CASSANDRA_4),
            new BasicVersionedMetadata("b", null, CASSANDRA_5),
            new BasicVersionedMetadata("c", CASSANDRA_5)
        ), CASSANDRA_4));
    }
}
