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
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.sql.DriverManager;

@Testcontainers
abstract class UsingDseContainerTest {

    static CassandraConnection sqlConnection = null;

    // Using @Container annotation restarts a new container for each test of the class, so as it takes ~20/30 sec. to
    // start a Cassandra container, we just want to have one container instance for all the tests of the class. See:
    // https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
    static CassandraContainer cassandraContainer;

    protected static void initializeContainer() {
        // For the official DataStax Enterprise server image, see here: https://hub.docker.com/r/datastax/dse-server/
        final DockerImageName dseServerImage = DockerImageName.parse("datastax/dse-server:7.0.0-a")
            .asCompatibleSubstituteFor("cassandra");
        cassandraContainer = new CassandraContainer(dseServerImage)
            .withEnv("DS_LICENSE", "accept")
            .withEnv("CLUSTER_NAME", "embedded_test_cluster")
            .withEnv("DC", "datacenter1")
            .withInitScript("initEmbeddedDse.cql");
        cassandraContainer.start();
    }

    @AfterAll
    static void afterTests() throws Exception {
        if (sqlConnection != null) {
            sqlConnection.close();
        }
        cassandraContainer.stop();
    }

    static void initConnection(final String keyspace, final String... parameters) throws Exception {
        sqlConnection = newConnection(keyspace, parameters);
    }

    static CassandraConnection newConnection(final String keyspace, final String... parameters) throws Exception {
        final InetSocketAddress contactPoint = cassandraContainer.getContactPoint();
        return (CassandraConnection) DriverManager.getConnection(buildJdbcUrl(contactPoint.getHostName(),
            contactPoint.getPort(), keyspace, parameters));
    }

    static String buildJdbcUrl(final String host, final int port, final String keyspace, final String... parameters) {
        String joinedParameters = String.join("&", parameters);
        if (StringUtils.isNotBlank(joinedParameters)) {
            joinedParameters = StringUtils.prependIfMissing(joinedParameters, "?");
        }

        return String.format("jdbc:cassandra://%s:%d/%s%s", host, port, keyspace, joinedParameters);
    }

}
