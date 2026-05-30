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

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import io.github.cdimascio.dotenv.Dotenv;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.profiles.ProfileProperty;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import static com.ing.data.cassandra.jdbc.utils.AwsUtil.AWS_SECRETSMANAGER_ENDPOINT_PROPERTY;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.JSSE_TRUSTSTORE_PASSWORD_PROPERTY;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.JSSE_TRUSTSTORE_PROPERTY;
import static org.apache.commons.lang3.StringUtils.isNoneBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static software.amazon.awssdk.profiles.ProfileProperty.AWS_ACCESS_KEY_ID;

/**
 * Test JDBC Driver against DBaaS Amazon Keyspaces.
 * <p>
 *     To run this test class, define the following environment variables in a .env file:
 *     <ul>
 *         <li><b>AWS_REGION</b>: The AWS region where the Keyspaces instance is deployed</li>
 *         <li><b>AWS_USER</b>: The username used to connect to Keyspaces (from specific credentials of the related user
 *         in the AWS IAM configuration)</li>
 *         <li><b>AWS_PASSWORD</b>: The password used to connect to Keyspaces (from specific credentials of the related
 *         user in the AWS IAM configuration)</li>
 *         <li><b>AWS_KEYSPACE_NAME</b>: The keyspace used for tests</li>
 *         <li><b>AWS_TRUSTSTORE_PATH</b>: The location of the trust store (JKS) including the Starfield digital
 *         certificate</li>
 *         <li><b>AWS_TRUSTSTORE_PASSWORD</b>: The password of the trust store aforementioned</li>
 *     </ul>
 *     Optionally, you can specify the following variables to use SigV4AuthProvider or retrieving password from the
 *     Amazon Secrets manager:
 *     <ul>
 *         <li><b>AWS_ACCESS_KEY</b>: The identifier of an AWS access key</li>
 *         <li><b>AWS_SECRET_ACCESS_KEY</b>: The AWS secret access key value</li>
 *     </ul>
 *     When retrieving the password from the Amazon Secrets manager, you must use the following environment variable to
 *     specify the name of the secret to retrieve in the Secrets manager: <b>AWS_SECRET_NAME</b>.
 * </p>
 */
@Disabled
@TestMethodOrder(org.junit.jupiter.api.MethodOrderer.OrderAnnotation.class)
@Testcontainers
@Slf4j
class AmazonKeyspacesIntegrationTest {

    private static final Dotenv DOTENV = Dotenv.load();
    private static final String AWS_REGION = DOTENV.get("AWS_REGION");
    private static final String AWS_USER = DOTENV.get("AWS_USER");
    private static final String AWS_PASSWORD = DOTENV.get("AWS_PASSWORD");
    private static final String AWS_KEYSPACE_NAME = DOTENV.get("AWS_KEYSPACE_NAME");
    private static final String AWS_SECRET_NAME = DOTENV.get("AWS_SECRET_NAME");
    private static final String AWS_ACCESS_KEY = DOTENV.get("AWS_ACCESS_KEY");
    private static final String AWS_SECRET_ACCESS_KEY = DOTENV.get("AWS_SECRET_ACCESS_KEY");
    private static final String AWS_TRUSTSTORE_PATH = DOTENV.get("AWS_TRUSTSTORE_PATH");
    private static final String AWS_TRUSTSTORE_PASSWORD = DOTENV.get("AWS_TRUSTSTORE_PASSWORD");
    private static final String SIMPLE_TABLE_NAME = "simple_table";

    /**
     * Integration with AWS Secrets manager uses a LocalStack container to limit the cost of testing this use case.
     */
    static final LocalStackContainer localStackContainer =
        new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
            .withEnv(AWS_ACCESS_KEY_ID.toUpperCase(Locale.ROOT), AWS_ACCESS_KEY)
            .withEnv(ProfileProperty.AWS_SECRET_ACCESS_KEY.toUpperCase(Locale.ROOT), AWS_SECRET_ACCESS_KEY)
            .withServices("secretsmanager");

    @BeforeAll
    static void setupAwsKeyspaces() {
        /* Valid Amazon Keyspaces endpoints:
         * 1. Global
         *    cassandra.us-east-1.amazonaws.com
         *    cassandra.us-east-1.api.aws
         *    cassandra-fips.us-east-1.amazonaws.com
         *    cassandra-fips.us-east-1.api.aws
         *    cassandra.us-east-2.amazonaws.com
         *    cassandra.us-east-2.api.aws
         *    cassandra.us-west-1.amazonaws.com
         *    cassandra.us-west-1.api.aws
         *    cassandra.us-west-2.amazonaws.com
         *    cassandra.us-west-2.api.aws
         *    cassandra-fips.us-west-2.amazonaws.com
         *    cassandra-fips.us-west-2.api.aws
         *    cassandra.af-south-1.amazonaws.com
         *    cassandra.af-south-1.api.aws
         *    cassandra.ap-east-1.amazonaws.com
         *    cassandra.ap-east-1.api.aws
         *    cassandra.ap-south-1.amazonaws.com
         *    cassandra.ap-south-1.api.aws
         *    cassandra.ap-northeast-1.amazonaws.com
         *    cassandra.ap-northeast-1.api.aws
         *    cassandra.ap-northeast-2.amazonaws.com
         *    cassandra.ap-northeast-2.api.aws
         *    cassandra.ap-southeast-1.amazonaws.com
         *    cassandra.ap-southeast-1.api.aws
         *    cassandra.ap-southeast-2.amazonaws.com
         *    cassandra.ap-southeast-2.api.aws
         *    cassandra.ca-central-1.amazonaws.com
         *    cassandra.ca-central-1.api.aws
         *    cassandra.eu-central-1.amazonaws.com
         *    cassandra.eu-central-1.api.aws
         *    cassandra.eu-west-1.amazonaws.com
         *    cassandra.eu-west-1.api.aws
         *    cassandra.eu-west-2.amazonaws.com
         *    cassandra.eu-west-2.api.aws
         *    cassandra.eu-west-3.amazonaws.com
         *    cassandra.eu-west-3.api.aws
         *    cassandra.eu-north-1.amazonaws.com
         *    cassandra.eu-north-1.api.aws
         *    cassandra.me-south-1.amazonaws.com
         *    cassandra.me-south-1.api.aws
         *    cassandra.me-central-1.amazonaws.com
         *    cassandra.me-central-1.api.aws
         *    cassandra.sa-east-1.amazonaws.com
         *    cassandra.sa-east-1.api.aws
         *
         * 2. AWS Gov Cloud (US)
         *    cassandra.us-gov-east-1.amazonaws.com
         *    cassandra.us-gov-east-1.api.aws
         *    cassandra.us-gov-west-1.amazonaws.com
         *    cassandra.us-gov-west-1.api.aws
         *
         * 3. China
         *    cassandra.cn-north-1.amazonaws.com.cn
         *    cassandra.cn-northwest-1.amazonaws.com.cn
         *
         * See: https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.endpoints.html (Jan. 2026)
         */

        if (canRunTests()) {
            log.debug("AWS_* variables are provided, Amazon Keyspaces integration tests will be executed.");

            /*
             * Configure truststore.
             * See: https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html#using_java_driver.BeforeYouBegin
             */
            System.setProperty(JSSE_TRUSTSTORE_PROPERTY, AWS_TRUSTSTORE_PATH);
            System.setProperty(JSSE_TRUSTSTORE_PASSWORD_PROPERTY, AWS_TRUSTSTORE_PASSWORD);

            if (canRunTestUsingSigV4() || canRunTestUsingSecret()) {
                /*
                 * Set the aws.accessKeyId and aws.secretKey generated for the user cassandra_test to be used by the
                 * DefaultAWSCredentialsProviderChain (used by SigV4AuthProvider).
                 * See: https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
                 */
                System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), AWS_ACCESS_KEY);
                System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), AWS_SECRET_ACCESS_KEY);
            }
        } else {
            log.debug("AWS_* variables are not defined, skipping Amazon Keyspaces integration tests.");
        }
    }

    static boolean canRunTests() {
        return isNoneBlank(
            AWS_REGION, AWS_USER, AWS_KEYSPACE_NAME, AWS_TRUSTSTORE_PATH, AWS_TRUSTSTORE_PASSWORD
        );
    }

    static boolean canRunTestsUsingPassword() {
        return canRunTests() && isNotBlank(AWS_PASSWORD);
    }

    static boolean canRunTestUsingSigV4() {
        return canRunTests() && isNoneBlank(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY);
    }

    static boolean canRunTestUsingSecret() {
        return canRunTestUsingSigV4() && isNotBlank(AWS_SECRET_NAME);
    }

    @Test
    @Order(1)
    @EnabledIf("canRunTestUsingSigV4")
    void testConnectionUsingSigV4AuthProvider() throws SQLException {
        final CassandraConnection connectionUsingSigV4 = (CassandraConnection) DriverManager.getConnection(
            "jdbc:cassandra:aws://cassandra." + AWS_REGION + ".amazonaws.com/" + AWS_KEYSPACE_NAME
                + "?usesigv4=true&awsregion=" + AWS_REGION);
        Assertions.assertNotNull(connectionUsingSigV4);

        assertConnectionIsOperational(connectionUsingSigV4);

        connectionUsingSigV4.close();
    }

    @Test
    @Order(2)
    @EnabledIf("canRunTestUsingSecret")
    void testConnectionUsingSecret() throws Exception {
        localStackContainer.start();
        System.setProperty(AWS_SECRETSMANAGER_ENDPOINT_PROPERTY, localStackContainer.getEndpoint().toString());
        localStackContainer.execInContainer("awslocal", "secretsmanager", "create-secret",
            "--name", AWS_SECRET_NAME, "--secret-string", AWS_PASSWORD);

        final CassandraConnection connectionUsingSecret = (CassandraConnection) DriverManager.getConnection(
            "jdbc:cassandra:aws://cassandra." + AWS_REGION + ".amazonaws.com:9142/" + AWS_KEYSPACE_NAME
                + "?awsregion=" + AWS_REGION
                + "&user=" + AWS_USER
                + "&awssecretregion=" + localStackContainer.getRegion()
                + "&awssecret=" + AWS_SECRET_NAME);

        Assertions.assertNotNull(connectionUsingSecret);

        assertConnectionIsOperational(connectionUsingSecret);

        connectionUsingSecret.close();
        localStackContainer.stop();
    }

    @Test
    @Order(3)
    @EnabledIf("canRunTestsUsingPassword")
    void testConnectionUsingPassword() throws Exception {
        /*
         * Create the connection to AWS Keyspaces, using the standard method with username/password :
         * https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html
         */
        final CassandraConnection sqlConnection = (CassandraConnection) DriverManager.getConnection(
            "jdbc:cassandra://cassandra." + AWS_REGION + ".amazonaws.com:9142/" + AWS_KEYSPACE_NAME
                + "?localdatacenter=" + AWS_REGION
                + "&user=" + AWS_USER
                + "&password=" + URLEncoder.encode(AWS_PASSWORD, StandardCharsets.UTF_8.name())
                + "&enablessl=true"
                + "&hostnameverification=false");

        Assertions.assertNotNull(sqlConnection);

        assertConnectionIsOperational(sqlConnection);

        sqlConnection.close();
    }

    private void assertConnectionIsOperational(final CassandraConnection sqlConnection) throws SQLException {
        createTable(sqlConnection);
        assertTrue(tableExists(sqlConnection));
        assertTrue(sqlConnection.isValid(3));
    }

    private void createTable(final Connection sqlConnection) throws SQLException {
        sqlConnection.createStatement().execute(SchemaBuilder
            .createTable(SIMPLE_TABLE_NAME)
            .ifNotExists()
            .withPartitionKey("email", DataTypes.TEXT)
            .withColumn("firstname", DataTypes.TEXT)
            .withColumn("lastname", DataTypes.TEXT)
            .build().getQuery());
    }

    private boolean tableExists(final CassandraConnection sqlConnection) throws SQLException {
        final String tableStatusCql =
            "SELECT status FROM system_schema_mcs.tables WHERE keyspace_name = ? AND table_name = ?";
        final CassandraPreparedStatement prepStatement = sqlConnection.prepareStatement(tableStatusCql);
        prepStatement.setString(1, AWS_KEYSPACE_NAME);
        prepStatement.setString(2, SIMPLE_TABLE_NAME);
        final ResultSet resultSet = prepStatement.executeQuery();
        return resultSet.next() && resultSet.getString(1) != null;
    }

    @AfterAll
    static void resetTrustStore() {
        System.clearProperty(JSSE_TRUSTSTORE_PROPERTY);
        System.clearProperty(JSSE_TRUSTSTORE_PASSWORD_PROPERTY);
    }

}


