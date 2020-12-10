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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


class UtilsUnitTest {

    static Stream<Arguments> buildUrlParsingTestCases() {
        return Stream.of(
            Arguments.of("jdbc:cassandra://localhost:9042/Keyspace1?version=3.0.0&consistency=QUORUM",
                new HashMap<String, String>() {{
                    put(Utils.TAG_SERVER_NAME, "localhost");
                    put(Utils.TAG_PORT_NUMBER, "9042");
                    put(Utils.TAG_DATABASE_NAME, "Keyspace1");
                    put(Utils.TAG_CQL_VERSION, "3.0.0");
                    put(Utils.TAG_CONSISTENCY_LEVEL, "QUORUM");
                }}),
            Arguments.of("jdbc:cassandra://localhost/Keyspace1?consistency=QUORUM",
                new HashMap<String, String>() {{
                    put(Utils.TAG_SERVER_NAME, "localhost");
                    put(Utils.TAG_PORT_NUMBER, "9042");
                    put(Utils.TAG_DATABASE_NAME, "Keyspace1");
                    put(Utils.TAG_CQL_VERSION, null);
                    put(Utils.TAG_CONSISTENCY_LEVEL, "QUORUM");
                }}),
            Arguments.of("jdbc:cassandra://localhost/Keyspace1?version=2.0.0",
                new HashMap<String, String>() {{
                    put(Utils.TAG_SERVER_NAME, "localhost");
                    put(Utils.TAG_PORT_NUMBER, "9042");
                    put(Utils.TAG_DATABASE_NAME, "Keyspace1");
                    put(Utils.TAG_CQL_VERSION, "2.0.0");
                    put(Utils.TAG_CONSISTENCY_LEVEL, null);
                }}),
            Arguments.of("jdbc:cassandra://localhost",
                new HashMap<String, String>() {{
                    put(Utils.TAG_SERVER_NAME, "localhost");
                    put(Utils.TAG_PORT_NUMBER, "9042");
                    put(Utils.TAG_DATABASE_NAME, null);
                    put(Utils.TAG_CQL_VERSION, null);
                    put(Utils.TAG_CONSISTENCY_LEVEL, null);
                }}),
            Arguments.of("jdbc:cassandra://localhost/Keyspace1?localdatacenter=DC1",
                new HashMap<String, String>() {{
                    put(Utils.TAG_SERVER_NAME, "localhost");
                    put(Utils.TAG_PORT_NUMBER, "9042");
                    put(Utils.TAG_DATABASE_NAME, "Keyspace1");
                    put(Utils.TAG_LOCAL_DATACENTER, "DC1");
                }})
        );
    }

    @ParameterizedTest
    @MethodSource("buildUrlParsingTestCases")
    void givenJdbcUrl_whenParseUrl_returnExpectedProperties(final String jdbcUrl,
                                                            final Map<String, String> expectedProperties)
        throws SQLException {
        final Properties result = Utils.parseURL(jdbcUrl);
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
        final Map<DriverOption, Object> policyOptions = Utils.parseReconnectionPolicy(policyString);
        assertNotNull(policyOptions);
        expectedPolicy.forEach((key, value) -> assertEquals(value, policyOptions.get(key)));
    }

    @Test
    void testCreateSubName() throws Exception {
        final String jdbcUrl = "jdbc:cassandra://localhost:9042/Keyspace1?consistency=QUORUM&version=3.0.0";
        final Properties props = Utils.parseURL(jdbcUrl);
        final String result = Utils.createSubName(props);
        assertEquals(jdbcUrl, Utils.PROTOCOL + result);
    }

}
