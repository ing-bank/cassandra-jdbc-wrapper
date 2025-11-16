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

package com.ing.data.cassandra.jdbc.utils;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.net.URI;
import java.sql.SQLTransientException;
import java.util.HashSet;
import java.util.Set;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.AWS_SECRET_RETRIEVAL_FAILED;
import static com.ing.data.cassandra.jdbc.utils.WarningConstants.INVALID_AWS_SECRETS_MANAGER_CUSTOM_ENDPOINT;

/**
 * Utility methods used for support of Amazon Keyspaces.
 */
@Slf4j
public final class AwsUtil {

    /**
     * Name of the system property used to override the default endpoint of the Amazon Secrets manager.
     */
    public static final String AWS_SECRETSMANAGER_ENDPOINT_PROPERTY = "aws.secretsmanager.endpoint";

    private AwsUtil() {
        // Private constructor to hide the public one.
    }


    /**
     * Lists all the valid hosts for Amazon Keyspaces.
     * <p>
     *     See: <a href="https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.endpoints.html">
     *     List of Amazon Keyspaces endpoints</a> (last update: March 2025).
     * </p>
     *
     * @return All the valid hosts for Amazon Keyspaces.
     */
    public static Set<String> listAwsKeyspacesHosts() {
        final Set<String> hosts = new HashSet<>();
        hosts.add("cassandra.us-east-1.amazonaws.com");
        hosts.add("cassandra-fips.us-east-1.amazonaws.com");
        hosts.add("cassandra.us-east-2.amazonaws.com");
        hosts.add("cassandra.us-west-1.amazonaws.com");
        hosts.add("cassandra.us-west-2.amazonaws.com");
        hosts.add("cassandra-fips.us-west-2.amazonaws.com");
        hosts.add("cassandra.af-south-1.amazonaws.com");
        hosts.add("cassandra.ap-east-1.amazonaws.com");
        hosts.add("cassandra.ap-south-1.amazonaws.com");
        hosts.add("cassandra.ap-northeast-1.amazonaws.com");
        hosts.add("cassandra.ap-northeast-2.amazonaws.com");
        hosts.add("cassandra.ap-southeast-1.amazonaws.com");
        hosts.add("cassandra.ap-southeast-2.amazonaws.com");
        hosts.add("cassandra.ca-central-1.amazonaws.com");
        hosts.add("cassandra.eu-central-1.amazonaws.com");
        hosts.add("cassandra.eu-west-1.amazonaws.com");
        hosts.add("cassandra.eu-west-2.amazonaws.com");
        hosts.add("cassandra.eu-west-3.amazonaws.com");
        hosts.add("cassandra.eu-north-1.amazonaws.com");
        hosts.add("cassandra.me-south-1.amazonaws.com");
        hosts.add("cassandra.sa-east-1.amazonaws.com");
        hosts.add("cassandra.us-gov-east-1.amazonaws.com");
        hosts.add("cassandra.us-gov-west-1.amazonaws.com");
        hosts.add("cassandra.cn-north-1.amazonaws.com.cn");
        hosts.add("cassandra.cn-northwest-1.amazonaws.com.cn");
        return hosts;
    }

    /**
     * Gets the value of the specified secret in the given AWS region.
     *
     * @param regionName The name of the AWS region.
     * @param secretName The name of the secret.
     * @return The value of the secret.
     * @throws SQLTransientException when retrieval of the secret in AWS Secret Manager failed.
     */
    public static String getSecretValue(final String regionName, final String secretName) throws SQLTransientException {
        final Region region = Region.of(regionName);
        final String customEndpoint = System.getProperty(AWS_SECRETSMANAGER_ENDPOINT_PROPERTY);
        final SecretsManagerClientBuilder secretsClientBuilder = SecretsManagerClient.builder()
            .region(region)
            .credentialsProvider(DefaultCredentialsProvider.builder().build());
        if (customEndpoint != null) {
            try {
                secretsClientBuilder.endpointOverride(URI.create(customEndpoint));
            } catch (final IllegalArgumentException e) {
                log.warn(INVALID_AWS_SECRETS_MANAGER_CUSTOM_ENDPOINT, e);
            }
        }
        final SecretsManagerClient secretsClient = secretsClientBuilder.build();
        return getValue(secretsClient, secretName);
    }

    private static String getValue(final SecretsManagerClient secretsClient,
                                   final String secretName) throws SQLTransientException {
        try {
            final GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                .secretId(secretName)
                .build();
            final GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
            final String secret = valueResponse.secretString();
            secretsClient.close();
            return secret;
        } catch (final SecretsManagerException e) {
            throw new SQLTransientException(String.format(AWS_SECRET_RETRIEVAL_FAILED, secretName,
                secretsClient.serviceClientConfiguration().region().id(), e.awsErrorDetails().errorMessage()), e);
        }
    }

}
