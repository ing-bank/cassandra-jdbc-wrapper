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
package com.ing.data.cassandra.jdbc.hcd;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.AuthConfig;
import com.ing.data.cassandra.jdbc.CassandraConnection;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.InetSocketAddress;
import java.sql.DriverManager;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.Strings.CS;

@Testcontainers
abstract class UsingHcdContainerTest {

    private static final Dotenv DOTENV = Dotenv.load();
    private static final String HCD_IMAGE = "cp.icr.io/cp/ibm-hyper-converged-database/hcd";
    private static final String HCD_TAG = "2.0.8-ubi";
    private static final String DATA_API_IMAGE = "cp.icr.io/cp/cpd/ibm-datastax-hcd/data-api";
    private static final String DATA_API_TAG = "v1.0.33";
    static CassandraConnection sqlConnection = null;
    static CassandraContainer hcdContainer;
    static GenericContainer<?> dataApiContainer;

    @BeforeAll
    static void pullImages() throws InterruptedException {
        // For the official HCD image, see here:
        // https://docs.datastax.com/en/hyper-converged-database/2.0/install/install-docker-developer.html
        // Download HCD and Data API images using the appropriate registry and credentials. Then, testcontainers will
        // not have to re-pull them (it avoids errors if the Docker Auth config on the machine running the tests is
        // not adapted to download images on cp.icr.io registry).
        final DockerClient client = DockerClientFactory.lazyClient();
        final AuthConfig dockerAuth = new AuthConfig()
            .withRegistryAddress("cp.icr.io")
            .withUsername("cp")
            .withPassword(DOTENV.get("HCD_ENTITLEMENT_KEY"));

        client.pullImageCmd(HCD_IMAGE)
            .withTag(HCD_TAG)
            .withAuthConfig(dockerAuth)
            .start()
            .awaitCompletion(1, TimeUnit.MINUTES);

        client.pullImageCmd(DATA_API_IMAGE)
            .withTag(DATA_API_TAG)
            .withAuthConfig(dockerAuth)
            .start()
            .awaitCompletion(1, TimeUnit.MINUTES);
    }

    protected static void initializeContainer() {
        final DockerImageName hcdImage =
            DockerImageName.parse(String.format("%s:%s", HCD_IMAGE, HCD_TAG)).asCompatibleSubstituteFor("cassandra");
        final Network hcdNetwork = Network.newNetwork();
        hcdContainer = new CassandraContainer(hcdImage)
            .withNetwork(hcdNetwork)
            .withCreateContainerCmdModifier(cmd -> memoryModifier(cmd, 3072L))
            .withCopyFileToContainer(MountableFile.forClasspathResource("cassandra-hcd.yaml"),
                "/opt/hcd/resources/cassandra/conf/cassandra.yaml")
            .withEnv("MAX_HEAP_SIZE", "1536M")
            .withEnv("CLUSTER_NAME", "hcd_cluster")
            .withEnv("DS_LICENSE", "accept")
            .withEnv("HCD_AUTO_CONF_OFF", "cassandra.yaml")
            .withEnv("JVM_EXTRA_OPTS", "-Dcassandra.sai.latest.version=ec")
            .withInitScript("initEmbeddedDse.cql");
        hcdContainer.start();

        dataApiContainer = new GenericContainer<>(
            DockerImageName.parse(String.format("%s:%s", DATA_API_IMAGE, DATA_API_TAG)))
            .withNetwork(hcdNetwork)
            .withExposedPorts(8181)
            .withCreateContainerCmdModifier(cmd -> memoryModifier(cmd, 2048L))
            .withEnv("JAVA_MAX_MEM_RATIO", "75")
            .withEnv("JAVA_INITIAL_MEM_RATIO", "50")
            .withEnv("GC_CONTAINER_OPTIONS", "-XX:+UseG1GC")
            .withEnv("STARGATE_JSONAPI_OPERATIONS_DATABASE_CONFIG_CASSANDRA_END_POINTS", "dc1")
            .withEnv("STARGATE_JSONAPI_OPERATIONS_DATABASE_CONFIG_LOCAL_DATACENTER", "hcd")
            .withEnv("QUARKUS_HTTP_ACCESS_LOG_ENABLED", "false")
            .withEnv("QUARKUS_LOG_LEVEL", "info")
            .withEnv("STARGATE_JSONAPI_OPERATIONS_VECTORIZE_ENABLED", "true")
            .withEnv("STARGATE_FEATURE_FLAGS_TABLES", "true")
            .withEnv("JAVA_OPTS_APPEND", "=-Dquarkus.http.host=0.0.0.0 "
                + "-Djava.util.logging.manager=org.jboss.logmanager.LogManager")
            .dependsOn(hcdContainer);
        dataApiContainer.start();
    }

    @AfterAll
    static void afterTests() throws Exception {
        if (sqlConnection != null) {
            sqlConnection.close();
        }
        dataApiContainer.stop();
        hcdContainer.stop();
    }

    private static void memoryModifier(final CreateContainerCmd createContainerCmd, final long memoryInMb) {
        if (createContainerCmd.getHostConfig() != null) {
            createContainerCmd.getHostConfig().withMemory(1024 * 1024 * memoryInMb);
        }
    }

    static void initConnection(final String keyspace, final String... parameters) throws Exception {
        sqlConnection = newConnection(keyspace, parameters);
    }

    static CassandraConnection newConnection(final String keyspace, final String... parameters) throws Exception {
        final InetSocketAddress contactPoint = hcdContainer.getContactPoint();
        return (CassandraConnection) DriverManager.getConnection(buildJdbcUrl(contactPoint.getHostName(),
            contactPoint.getPort(), keyspace, parameters));
    }

    static String buildJdbcUrl(final String host, final int port, final String keyspace, final String... parameters) {
        String joinedParameters = String.join("&", parameters);
        if (StringUtils.isNotBlank(joinedParameters)) {
            joinedParameters = CS.prependIfMissing(joinedParameters, "?");
        }

        return String.format("jdbc:cassandra://%s:%d/%s%s", host, port, keyspace, joinedParameters);
    }

}
