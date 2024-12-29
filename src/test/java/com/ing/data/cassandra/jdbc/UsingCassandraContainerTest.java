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

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.InetSocketAddress;
import java.sql.DriverManager;

@Testcontainers
public abstract class UsingCassandraContainerTest {

    // For the official Cassandra image, see here: https://hub.docker.com/_/cassandra
    static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse("cassandra:5");

    protected static CassandraConnection sqlConnection = null;

    // Using @Container annotation restarts a new container for each test of the class, so as it takes ~20/30 sec. to
    // start a Cassandra container, we just want to have one container instance for all the tests of the class. See:
    // https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
    static final CassandraContainer cassandraContainer = new CassandraContainer(CASSANDRA_IMAGE)
        .withEnv("CASSANDRA_DC", "datacenter1")
        .withEnv("CASSANDRA_CLUSTER_NAME", "embedded_test_cluster")
        .withCopyFileToContainer(MountableFile.forClasspathResource("cassandra.keystore"),
            "/security/cassandra.keystore")
        .withCopyFileToContainer(MountableFile.forClasspathResource("cassandra.truststore"),
            "/security/cassandra.truststore")
        .withConfigurationOverride("config_override")
        .withInitScript("initEmbeddedCassandra.cql");

    @BeforeAll
    static void setUpTests() {
        cassandraContainer.start();
    }

    @AfterAll
    static void afterTests() throws Exception {
        if (sqlConnection != null) {
            sqlConnection.close();
        }
        cassandraContainer.stop();
    }

    protected static void initConnection(final String keyspace, final String... parameters) throws Exception {
        sqlConnection = newConnection(keyspace, false, parameters);
    }

    static void initConnectionUsingIpV6(final String keyspace, final String... parameters) throws Exception {
        sqlConnection = newConnection(keyspace, true, parameters);
    }

    static CassandraConnection newConnection(final String keyspace, final String... parameters) throws Exception {
        return newConnection(keyspace, false, parameters);
    }

    static CassandraConnection newConnection(final String keyspace, final boolean usingIpV6,
                                             final String... parameters) throws Exception {
        final InetSocketAddress contactPoint = cassandraContainer.getContactPoint();
        String host = contactPoint.getHostName();
        if (usingIpV6 && contactPoint.getHostName().contains("localhost")) {
            host = "[::1]";
        }
        return (CassandraConnection) DriverManager.getConnection(buildJdbcUrl(host, contactPoint.getPort(),
            keyspace, parameters));
    }

    static String buildJdbcUrl(final String host, final int port, final String keyspace, final String... parameters) {
        String joinedParameters = String.join("&", parameters);
        if (StringUtils.isNotBlank(joinedParameters)) {
            joinedParameters = StringUtils.prependIfMissing(joinedParameters, "?");
        }

        return String.format("jdbc:cassandra://%s:%d/%s%s", host, port, keyspace, joinedParameters);
    }

}
