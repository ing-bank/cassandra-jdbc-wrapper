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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import com.ing.data.cassandra.jdbc.optionset.Liquibase;
import com.ing.data.cassandra.jdbc.utils.ContactPoint;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.ShardingKey;
import java.util.List;

import static com.ing.data.cassandra.jdbc.utils.WarningConstants.UNSUPPORTED_SHARDING_KEYS;

/**
 * Cassandra connection builder: implementation class for {@link ConnectionBuilder}.
 */
@Slf4j
public class CassandraConnectionBuilder implements ConnectionBuilder {

    private final CassandraDataSource dataSource;

    CassandraConnectionBuilder(final CassandraDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public CassandraConnectionBuilder user(final String username) {
        return runThenReturnBuilder(() -> this.dataSource.setUser(username));
    }

    @Override
    public CassandraConnectionBuilder password(final String password) {
        return runThenReturnBuilder(() -> this.dataSource.setPassword(password));
    }

    /**
     * Specifies the contact points to be used when creating a connection.
     *
     * @param contactPoints The contact points to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder contactPoints(final List<ContactPoint> contactPoints) {
        return runThenReturnBuilder(() -> this.dataSource.setContactPoints(contactPoints));
    }

    /**
     * Specifies the AstraDB database name to be used when creating a connection.
     *
     * @param astraDatabaseName The AstraDB database name to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder astraDatabaseName(final String astraDatabaseName) {
        return runThenReturnBuilder(() -> this.dataSource.setAstraDatabaseName(astraDatabaseName));
    }

    /**
     * Specifies the database name (in case of Cassandra, i.e. the keyspace used as data source) to be used when
     * creating a connection.
     * <p>
     *     Warning: for connections to AstraDB instances, the database name must be set with
     *     {@link #astraDatabaseName(String)}.
     * </p>
     *
     * @param databaseName The database name to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder databaseName(final String databaseName) {
        return runThenReturnBuilder(() -> this.dataSource.setDatabaseName(databaseName));
    }

    /**
     * Specifies the compliance mode to be used when creating a connection.
     *
     * @param complianceMode The compliance mode to use for this connection (for example, {@link Liquibase}).
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder complianceMode(final String complianceMode) {
        return runThenReturnBuilder(() -> this.dataSource.setComplianceMode(complianceMode));
    }

    /**
     * Specifies the consistency level to be used when creating a connection.
     * <p>
     *     See <a href="https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html">
     *         consistency level documentation</a> and {@link ConsistencyLevel} to get the acceptable values.
     * </p>
     *
     * @param consistency The consistency level to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder consistency(final String consistency) {
        return runThenReturnBuilder(() -> this.dataSource.setConsistency(consistency));
    }

    /**
     * Specifies the serial consistency level to be used when creating a connection.
     * <p>
     *     The acceptable values are {@link ConsistencyLevel#SERIAL} and {@link ConsistencyLevel#LOCAL_SERIAL}. See
     *     <a href="https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigSerialConsistency.html">
     *     serial consistency level documentation</a> for further details.
     * </p>
     *
     * @param consistency The serial consistency level to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder serialConsistency(final String consistency) {
        return runThenReturnBuilder(() -> this.dataSource.setSerialConsistency(consistency));
    }

    /**
     * Specifies the local datacenter to be used when creating a connection. It is required with the
     * {@link DefaultLoadBalancingPolicy}.
     *
     * @param localDataCenter The local datacenter to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder localDataCenter(final String localDataCenter) {
        return runThenReturnBuilder(() -> this.dataSource.setLocalDataCenter(localDataCenter));
    }

    /**
     * Specifies the execution profile to be used when creating a connection.
     *
     * @param profile The execution profile to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder activeProfile(final String profile) {
        return runThenReturnBuilder(() -> this.dataSource.setActiveProfile(profile));
    }

    /**
     * Specifies the retry policy to be used when creating a connection.
     * <p>
     *     The value must be the full package of a policy's class implementing {@link RetryPolicy} interface.
     *     See <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/retries/">
     *     Retries documentation</a> for further information.
     * </p>
     *
     * @param policy The retry policy to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder retryPolicy(final String policy) {
        return runThenReturnBuilder(() -> this.dataSource.setRetryPolicy(policy));
    }

    /**
     * Specifies the load balancing policy to be used when creating a connection.
     * <p>
     *     The value must be the full package of a policy's class implementing {@link LoadBalancingPolicy} interface.
     *     See <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/load_balancing/">
     *     Load balancing documentation</a> for further information.
     * </p>
     *
     * @param policy The load balancing policy to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder loadBalancingPolicy(final String policy) {
        return runThenReturnBuilder(() -> this.dataSource.setLoadBalancingPolicy(policy));
    }

    /**
     * Specifies the reconnection policy to be used when creating a connection.
     * <p>
     *     If you want to define a custom base delay (in seconds, by default 1 second) and a custom max delay
     *     (in seconds, by default 60 seconds) for the default {@link ExponentialReconnectionPolicy}, you can specify
     *     it as following: {@code ExponentialReconnectionPolicy((long)<delay>,(long)<maxDelay>)}.
     *     You can also use the {@link ConstantReconnectionPolicy} as policy using {@code ConstantReconnectionPolicy()}
     *     or with a custom base delay (in seconds, by default 1 second):
     *     {@code ConstantReconnectionPolicy((long)<baseDelay>)}.
     *     Make sure you always cast the policy's arguments appropriately.
     * </p>
     * <p>
     *     If you want to use a custom policy, specify the full package of the policy's class.
     * </p>
     * <p>
     *     See <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/reconnection/">
     *     Reconnection documentation</a> for further information.
     * </p>
     *
     * @param policy The reconnection policy to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder reconnectionPolicy(final String policy) {
        return runThenReturnBuilder(() -> this.dataSource.setReconnectionPolicy(policy));
    }

    /**
     * Specifies the default fetch size for all the queries returning result sets, to be used when creating a
     * connection.
     * <p>
     *     This value is the number of rows the server will return in each network frame. It corresponds to the
     *     property {@code datastax-java-driver.basic.request.page-size}. See the documentation about the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/paging/">paging</a> for
     *     further details.
     * </p>
     *
     * @param fetchSize The fetch size to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder fetchSize(final Integer fetchSize) {
        return runThenReturnBuilder(() -> this.dataSource.setFetchSize(fetchSize));
    }

    /**
     * Specifies whether the secured traffic is enabled when creating a connection.
     * <p>
     *     When enabled, the default SSL engine {@link DefaultSslEngineFactory} is used with the JSSE system properties
     *     defined for your application.
     * </p>
     *
     * @param enabled Whether the secured traffic is enabled for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder sslEnabled(final boolean enabled) {
        return runThenReturnBuilder(() -> this.dataSource.setSslEnabled(enabled));
    }

    /**
     * Specifies the SSL engine factory to be used when creating a connection.
     * <p>
     *     The value must be the fully-qualified name of a class with a no-args constructor implementing
     *     {@link SslEngineFactory} interface.
     * </p>
     *
     * @param factory The SSL engine factory to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder sslEngineFactory(final String factory) {
        return runThenReturnBuilder(() -> this.dataSource.setSslEngineFactory(factory));
    }

    /**
     * Specifies whether the validation of the server certificate's common name against the hostname of the server being
     * connected is enabled when creating a connection.
     * <p>
     *     When enabled, the default SSL engine {@link DefaultSslEngineFactory} is used with the JSSE system properties
     *     defined for your application.
     * </p>
     *
     * @param enabled Whether the hostname verification is enabled for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder hostnameVerified(final boolean enabled) {
        return runThenReturnBuilder(() -> this.dataSource.setHostnameVerified(enabled));
    }

    /**
     * Specifies the fully qualified path of the cloud secure connect bundle file used to connect to an AstraDB
     * instance, to be used when creating a connection.
     *
     * @param bundlePath The fully qualified path of the cloud secure connect bundle file to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder secureConnectBundle(final String bundlePath) {
        return runThenReturnBuilder(() -> this.dataSource.setSecureConnectBundle(bundlePath));
    }

    /**
     * Specifies the path of the cloud secure connect bundle file used to connect to an AstraDB instance, to be used
     * when creating a connection.
     *
     * @param bundlePath The path of the cloud secure connect bundle file to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder secureConnectBundle(final Path bundlePath) {
        return runThenReturnBuilder(() -> this.dataSource.setSecureConnectBundle(bundlePath));
    }

    /**
     * Specifies the token used to connect to an AstraDB instance, to be used when creating a connection.
     *
     * @param token The token to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder token(final String token) {
        return runThenReturnBuilder(() -> this.dataSource.setToken(token));
    }

    /**
     * Specifies the region of the AstraDB instance, to be used when creating a connection.
     *
     * @param region The AstraDB region to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder astraRegion(final String region) {
        return runThenReturnBuilder(() -> this.dataSource.setAstraRegion(region));
    }

    /**
     * Specifies whether the Kerberos auth provider is enabled when creating a connection.
     * <p>
     *     This will enable the Kerberos {@link AuthProvider} implementation for the connection with default parameters.
     *     See <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/authentication/">
     *     Authentication reference</a> and <a href="https://github.com/instaclustr/cassandra-java-driver-kerberos">
     *     Kerberos authenticator for Java driver</a> for further information.
     * </p>
     *
     * @param enabled Whether the Kerberos auth provider is enabled for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder kerberosAuthProviderEnabled(final boolean enabled) {
        return runThenReturnBuilder(() -> this.dataSource.setKerberosAuthProviderEnabled(enabled));
    }

    /**
     * Specifies the request timeout in milliseconds to be used when creating a connection.
     * <p>
     *     It corresponds to the property {@code basic.request.timeout} in the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference">
     *     Configuration reference page</a>.
     * </p>
     *
     * @param timeout The request timeout in milliseconds to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder requestTimeout(final Long timeout) {
        return runThenReturnBuilder(() -> this.dataSource.setRequestTimeout(timeout));
    }

    /**
     * Specifies the connection timeout in milliseconds to be used when creating a connection.
     * <p>
     *     It corresponds to the property {@code advanced.connection.connect-timeout} in the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference">
     *     Configuration reference page</a>.
     * </p>
     *
     * @param timeout The connection timeout in milliseconds to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder connectionTimeout(final Long timeout) {
        return runThenReturnBuilder(() -> this.dataSource.setConnectionTimeout(timeout));
    }

    /**
     * Specifies whether the <a href="https://en.wikipedia.org/wiki/Nagle%27s_algorithm">Nagle's algorithm</a> is
     * enabled when creating a connection.
     * <p>
     *     It corresponds to the property {@code advanced.socket.tcp-no-delay} in the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference">
     *     Configuration reference page</a>.
     * </p>
     *
     * @param enabled Whether the Nagle's algorithm is enabled for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder tcpNoDelayEnabled(final boolean enabled) {
        return runThenReturnBuilder(() -> this.dataSource.setTcpNoDelayEnabled(enabled));
    }

    /**
     * Specifies whether the TCP keep-alive is enabled when creating a connection.
     * <p>
     *     It corresponds to the property {@code advanced.socket.keep-alive} in the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference">
     *     Configuration reference page</a>.
     * </p>
     *
     * @param enabled Whether the TCP keep-alive is enabled for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder tcpKeepAliveEnabled(final boolean enabled) {
        return runThenReturnBuilder(() -> this.dataSource.setTcpKeepAliveEnabled(enabled));
    }

    /**
     * Specifies the fully qualified path of the
     * <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference/">
     *     client configuration file</a> to be used when creating a connection.
     *
     * @param configurationFile The fully qualified path of the client configuration file to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder configurationFile(final String configurationFile) {
        return runThenReturnBuilder(() -> this.dataSource.setConfigurationFile(configurationFile));
    }

    /**
     * Specifies the path of the
     * <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference/">
     *     client configuration file</a> to be used when creating a connection.
     *
     * @param configurationFilePath The path of the client configuration file to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder configurationFile(final Path configurationFilePath) {
        return runThenReturnBuilder(() -> this.dataSource.setConfigurationFile(configurationFilePath));
    }

    /**
     * Specifies the AWS region of the contact point of the Amazon Keyspaces instance, to be used when creating a
     * connection.
     *
     * @param region The string representation of the AWS region to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder awsRegion(final String region) {
        return runThenReturnBuilder(() -> this.dataSource.setAwsRegion(region));
    }

    /**
     * Specifies the AWS region of the contact point of the Amazon Keyspaces instance, to be used when creating a
     * connection.
     *
     * @param region The AWS region to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder awsRegion(final Region region) {
        return runThenReturnBuilder(() -> this.dataSource.setAwsRegion(region));
    }

    /**
     * Specifies the AWS region of the Amazon Secret Manager in which the credentials of the user used for the
     * connection are stored, to be used when creating a connection.
     *
     * @param region The string representation of the AWS region to use to retrieve the credentials in Amazon Secret
     *               Manager for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder awsSecretRegion(final String region) {
        return runThenReturnBuilder(() -> this.dataSource.setAwsSecretRegion(region));
    }

    /**
     * Specifies the AWS region of the Amazon Secret Manager in which the credentials of the user used for the
     * connection are stored, to be used when creating a connection.
     *
     * @param region The AWS region to use to retrieve the credentials in Amazon Secret Manager for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder awsSecretRegion(final Region region) {
        return runThenReturnBuilder(() -> this.dataSource.setAwsSecretRegion(region));
    }

    /**
     * Specifies the name of the secret, stored in Amazon Secret Manager, containing the credentials of the user used
     * for the connection, to be used when creating a connection.
     *
     * @param secretName The name of the secret to use for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder awsSecretName(final String secretName) {
        return runThenReturnBuilder(() -> this.dataSource.setAwsSecretName(secretName));
    }

    /**
     * Specifies whether the Amazon Signature V4 auth provider is enabled when creating a connection.
     * <p>
     *     This will enable the Amazon Signature V4 {@link AuthProvider} implementation for the connection using the
     *     AWS region defined in the property {@link #awsRegion(String)} (or {@link #awsRegion(Region)}).
     *     See <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/authentication/">
     *     Authentication reference</a> and
     *     <a href="https://github.com/aws/aws-sigv4-auth-cassandra-java-driver-plugin">
     *     Amazon Signature V4 authenticator plugin for Java driver</a> for further information.
     * </p>
     *
     * @param enabled Whether the Amazon Signature V4 auth provider is enabled for this connection.
     * @return The same {@link ConnectionBuilder} instance.
     */
    public CassandraConnectionBuilder sigV4AuthProviderEnabled(final boolean enabled) {
        return runThenReturnBuilder(() -> this.dataSource.setSigV4AuthProviderEnabled(enabled));
    }

    @Override
    public CassandraConnectionBuilder shardingKey(final ShardingKey shardingKey) {
        // Sharding keys are not supported, this is a no-op method.
        return runThenReturnBuilder(() -> log.warn(UNSUPPORTED_SHARDING_KEYS));
    }

    @Override
    public CassandraConnectionBuilder superShardingKey(final ShardingKey shardingKey) {
        // Super sharding keys are not supported, this is a no-op method.
        return runThenReturnBuilder(() -> log.warn(UNSUPPORTED_SHARDING_KEYS));
    }

    @Override
    public Connection build() throws SQLException {
        return this.dataSource.getConnection();
    }

    private CassandraConnectionBuilder runThenReturnBuilder(final Runnable operation) {
        operation.run();
        return this;
    }

}
