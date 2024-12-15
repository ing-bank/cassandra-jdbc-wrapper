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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.net.URI;
import java.sql.SQLTransientException;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.AWS_SECRET_RETRIEVAL_FAILED;

/**
 * Utility methods used for support of Amazon Keyspaces.
 */
public final class AwsUtil {

    /**
     * Regular expression matching the valid hosts for Amazon Keyspaces.
     */
    public static final String AWS_KEYSPACES_HOSTS_REGEX =
        "cassandra(-fips)?\\.(us(-gov)?|ap|eu|ca|me|sa|cn)-central|(north|south)?(east|west)?-[0-9]"
            + "\\.amazonaws\\.com(\\.cn)?";

    /**
     * Name of the system property used to override the default endpoint of the Amazon Secrets manager.
     */
    public static final String AWS_SECRETSMANAGER_ENDPOINT_PROPERTY = "aws.secretsmanager.endpoint";

    static final Logger LOG = LoggerFactory.getLogger(AwsUtil.class);

    private AwsUtil() {
        // Private constructor to hide the public one.
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
        final SecretsManagerClientBuilder secretsClientBuilder = SecretsManagerClient.builder().region(region);
        if (customEndpoint != null) {
            try {
                secretsClientBuilder.endpointOverride(URI.create(customEndpoint));
            } catch (final IllegalArgumentException e) {
                LOG.warn("Unable to set a custom endpoint for Amazon Secrets manager, will fallback to the "
                    + "default endpoint.", e);
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
