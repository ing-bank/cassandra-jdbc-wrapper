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
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import com.ing.data.cassandra.jdbc.optionset.Default;
import com.ing.data.cassandra.jdbc.optionset.Liquibase;
import com.ing.data.cassandra.jdbc.utils.ContactPoint;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.regions.Region;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import static com.ing.data.cassandra.jdbc.CassandraConnection.FALLBACK_FETCH_SIZE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NO_INTERFACE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.PROTOCOL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_ACTIVE_PROFILE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_ASTRA_DATABASE_NAME;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_ASTRA_REGION;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_AWS_REGION;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_AWS_SECRET_NAME;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_AWS_SECRET_REGION;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CLOUD_SECURE_CONNECT_BUNDLE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_COMPLIANCE_MODE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONFIG_FILE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONNECT_TIMEOUT;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONSISTENCY_LEVEL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONTACT_POINTS;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_DATABASE_NAME;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_ENABLE_SSL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_FETCH_SIZE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_KEEP_ALIVE;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_LOAD_BALANCING_POLICY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_LOCAL_DATACENTER;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_PASSWORD;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_RECONNECT_POLICY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_REQUEST_TIMEOUT;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_RETRY_POLICY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_SERIAL_CONSISTENCY_LEVEL;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_SSL_ENGINE_FACTORY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_SSL_HOSTNAME_VERIFICATION;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_TCP_NO_DELAY;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_TOKEN;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_USER;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_USE_KERBEROS;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_USE_SIG_V4;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.createSubName;

/**
 * Cassandra data source: implementation class for {@link DataSource} and {@link ConnectionPoolDataSource}.
 */
public class CassandraDataSource implements ConnectionPoolDataSource, DataSource {

    // Check the driver.
    static {
        try {
            Class.forName("com.ing.data.cassandra.jdbc.CassandraDriver");
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The Cassandra data source description.
     */
    protected static final String DATA_SOURCE_DESCRIPTION = "Cassandra Data Source";

    private final Properties properties = new Properties();
    private String url;

    @SuppressWarnings("unused")
    private CassandraDataSource() {
        // Hide the default constructor to force setting contact points and keyspace when creating the data source.
    }

    /**
     * Constructor.
     *
     * @param contactPoints The contact points.
     * @param keyspace      The keyspace.
     */
    public CassandraDataSource(final List<ContactPoint> contactPoints, final String keyspace) {
        this.setContactPoints(contactPoints);
        this.setDatabaseName(keyspace);
    }

    @Override
    public CassandraConnection getConnection() throws SQLException {
        return getConnection(null, null);
    }

    @Override
    public CassandraConnection getConnection(final String user, final String password) throws SQLException {
        if (user != null) {
            this.setUser(user);
        }
        if (password != null) {
            this.setPassword(password);
        }
        if (StringUtils.isBlank(this.url)) {
            this.url = PROTOCOL.concat(createSubName(this.properties));
        }
        return (CassandraConnection) DriverManager.getConnection(this.url, convertAllPropertiesToString());
    }

    @Override
    public int getLoginTimeout() {
        return DriverManager.getLoginTimeout();
    }

    @Override
    public PrintWriter getLogWriter() {
        return DriverManager.getLogWriter();
    }

    @Override
    public void setLoginTimeout(final int timeout) {
        DriverManager.setLoginTimeout(timeout);
    }

    @Override
    public void setLogWriter(final PrintWriter writer) {
        DriverManager.setLogWriter(writer);
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(this.getClass());
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        } else {
            throw new SQLException(String.format(NO_INTERFACE, iface.getSimpleName()));
        }
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public PooledCassandraConnection getPooledConnection() throws SQLException {
        return new PooledCassandraConnection(getConnection());
    }

    @Override
    public PooledCassandraConnection getPooledConnection(final String user, final String password) throws SQLException {
        return new PooledCassandraConnection(getConnection(user, password));
    }

    /**
     * Sets the {@link DriverManager} URL used to establish the connection.
     *
     * @param url The {@link DriverManager} URL used to establish the connection.
     */
    public void setURL(final String url) {
        this.url = url;
    }

    /**
     * Gets the data source description.
     *
     * @return The data source description.
     */
    public String getDescription() {
        return DATA_SOURCE_DESCRIPTION;
    }

    /**
     * Gets the contact points of the data source.
     *
     * @return The contact points of the data source.
     */
    @SuppressWarnings("unchecked")
    public List<ContactPoint> getContactPoints() {
        return (List<ContactPoint>) this.properties.getOrDefault(TAG_CONTACT_POINTS, new ArrayList<>());
    }

    /**
     * Sets the contact points of the data source.
     *
     * @param contactPoints The contact points of the data source.
     */
    public void setContactPoints(final List<ContactPoint> contactPoints) {
        this.setDataSourceProperty(TAG_CONTACT_POINTS, contactPoints);
    }

    /**
     * Gets the AstraDB database name.
     *
     * @return The AstraDB database name.
     */
    public String getAstraDatabaseName() {
        return this.properties.getProperty(TAG_ASTRA_DATABASE_NAME);
    }

    /**
     * Sets the AstraDB database name.
     *
     * @param astraDatabaseName The AstraDB database name.
     */
    public void setAstraDatabaseName(final String astraDatabaseName) {
        this.setDataSourceProperty(TAG_ASTRA_DATABASE_NAME, astraDatabaseName);
    }

    /**
     * Gets the database name. In case of Cassandra, i.e. the keyspace used as data source.
     * <p>
     *     Warning: for connections to AstraDB instances, the database name must be retrieved with
     *     {@link #getAstraDatabaseName()}.
     * </p>
     *
     * @return The database name. In case of Cassandra, i.e. the keyspace used as data source.
     */
    public String getDatabaseName() {
        return this.properties.getProperty(TAG_DATABASE_NAME);
    }

    /**
     * Sets the database name. In case of Cassandra, i.e. the keyspace used as data source.
     * <p>
     *     Warning: for connections to AstraDB instances, the database name must be set with
     *     {@link #setAstraDatabaseName(String)}.
     * </p>
     *
     * @param databaseName The database name. In case of Cassandra, i.e. the keyspace used as data source.
     */
    public void setDatabaseName(final String databaseName) {
        this.setDataSourceProperty(TAG_DATABASE_NAME, databaseName);
    }

    /**
     * Gets the password used to connect to the data source.
     *
     * @return The password used to connect to the data source.
     */
    public String getPassword() {
        return this.properties.getProperty(TAG_PASSWORD);
    }

    /**
     * Sets the password used to connect to the data source.
     *
     * @param password The password used to connect to the data source.
     */
    public void setPassword(final String password) {
        this.setDataSourceProperty(TAG_PASSWORD, password);
    }

    /**
     * Gets the username used to connect to the data source.
     *
     * @return The username used to connect to the data source.
     */
    public String getUser() {
        return this.properties.getProperty(TAG_USER);
    }

    /**
     * Sets the username used to connect to the data source.
     *
     * @param user The username used to connect to the data source.
     */
    public void setUser(final String user) {
        this.setDataSourceProperty(TAG_USER, user);
    }

    /**
     * Gets the compliance mode.
     * <p>
     *     The default value is {@link Default}.
     * </p>
     *
     * @return The compliance mode to use for the connection (for example, {@link Liquibase}).
     */
    public String getComplianceMode() {
        return this.properties.getProperty(TAG_COMPLIANCE_MODE, Default.class.getSimpleName());
    }

    /**
     * Sets the compliance mode.
     *
     * @param complianceMode The compliance mode to use for the connection (for example, {@link Liquibase}).
     */
    public void setComplianceMode(final String complianceMode) {
        this.setDataSourceProperty(TAG_COMPLIANCE_MODE, complianceMode);
    }

    /**
     * Gets the consistency level.
     * <p>
     *     See <a href="https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html">
     *     consistency level documentation</a> for further details. The default consistency level is
     *     {@link ConsistencyLevel#LOCAL_ONE}.
     * </p>
     *
     * @return The consistency level.
     */
    public String getConsistency() {
        return this.properties.getProperty(TAG_CONSISTENCY_LEVEL, ConsistencyLevel.LOCAL_ONE.name());
    }

    /**
     * Sets the consistency level.
     * <p>
     *     See <a href="https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html">
     *     consistency level documentation</a> and {@link ConsistencyLevel} to get the acceptable values.
     * </p>
     *
     * @param consistency The consistency level.
     */
    public void setConsistency(final String consistency) {
        this.setDataSourceProperty(TAG_CONSISTENCY_LEVEL, consistency);
    }

    /**
     * Gets the serial consistency level.
     * <p>
     *     See <a href="https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigSerialConsistency.html">
     *     serial consistency level documentation</a> for further details. The default serial consistency level is
     *     {@link ConsistencyLevel#SERIAL}.
     * </p>
     *
     * @return The serial consistency level.
     */
    public String getSerialConsistency() {
        return this.properties.getProperty(TAG_SERIAL_CONSISTENCY_LEVEL, ConsistencyLevel.SERIAL.name());
    }

    /**
     * Sets the serial consistency level.
     * <p>
     *     The acceptable values are {@link ConsistencyLevel#SERIAL} and {@link ConsistencyLevel#LOCAL_SERIAL}. See
     *     <a href="https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigSerialConsistency.html">
     *     serial consistency level documentation</a> for further details.
     * </p>
     *
     * @param consistency The serial consistency level.
     */
    public void setSerialConsistency(final String consistency) {
        this.setDataSourceProperty(TAG_SERIAL_CONSISTENCY_LEVEL, consistency);
    }

    /**
     * Gets the local datacenter. It is required with the {@link DefaultLoadBalancingPolicy}.
     *
     * @return The local datacenter.
     */
    public String getLocalDataCenter() {
        return this.properties.getProperty(TAG_LOCAL_DATACENTER);
    }

    /**
     * Sets the local datacenter. It is required with the {@link DefaultLoadBalancingPolicy}.
     *
     * @param localDataCenter The local datacenter.
     */
    public void setLocalDataCenter(final String localDataCenter) {
        this.setDataSourceProperty(TAG_LOCAL_DATACENTER, localDataCenter);
    }

    /**
     * Gets the execution profile to use when the connection is created.
     * <p>
     *     The default value is {@link DriverExecutionProfile#DEFAULT_NAME}.
     * </p>
     *
     * @return The execution profile to use for the connection.
     */
    public String getActiveProfile() {
        return this.properties.getProperty(TAG_ACTIVE_PROFILE, DriverExecutionProfile.DEFAULT_NAME);
    }

    /**
     * Sets the execution profile to use when the connection is created.
     *
     * @param profile The execution profile to use for the connection.
     */
    public void setActiveProfile(final String profile) {
        this.setDataSourceProperty(TAG_ACTIVE_PROFILE, profile);
    }

    /**
     * Gets the retry policy.
     * <p>
     *     The value must be the full package of a policy's class implementing {@link RetryPolicy} interface.
     *     By default, the driver will use {@link DefaultRetryPolicy} (see
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/retries/">
     *     Retries documentation</a>).
     * </p>
     *
     * @return The retry policy.
     */
    public String getRetryPolicy() {
        return this.properties.getProperty(TAG_RETRY_POLICY, DefaultRetryPolicy.class.getSimpleName());
    }

    /**
     * Sets the retry policy.
     * <p>
     *     The value must be the full package of a policy's class implementing {@link RetryPolicy} interface.
     *     See <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/retries/">
     *     Retries documentation</a> for further information.
     * </p>
     *
     * @param policy The retry policy.
     */
    public void setRetryPolicy(final String policy) {
        this.setDataSourceProperty(TAG_RETRY_POLICY, policy);
    }

    /**
     * Gets the load balancing policy.
     * <p>
     *     The value must be the full package of a policy's class implementing {@link LoadBalancingPolicy} interface.
     *     By default, the driver will use {@link DefaultLoadBalancingPolicy} (see
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/load_balancing/">
     *     Load balancing documentation</a>).
     * </p>
     *
     * @return The load balancing policy.
     */
    public String getLoadBalancingPolicy() {
        return this.properties.getProperty(TAG_LOAD_BALANCING_POLICY, DefaultLoadBalancingPolicy.class.getSimpleName());
    }

    /**
     * Sets the load balancing policy.
     * <p>
     *     The value must be the full package of a policy's class implementing {@link LoadBalancingPolicy} interface.
     *     See <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/load_balancing/">
     *     Load balancing documentation</a> for further information.
     * </p>
     *
     * @param policy The load balancing policy.
     */
    public void setLoadBalancingPolicy(final String policy) {
        this.setDataSourceProperty(TAG_LOAD_BALANCING_POLICY, policy);
    }

    /**
     * Gets the reconnection policy.
     * <p>
     *     The default value is {@link ExponentialReconnectionPolicy} with default values (see
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/reconnection/">
     *     Reconnection documentation</a>).
     * </p>
     *
     * @return The reconnection policy.
     */
    public String getReconnectionPolicy() {
        return this.properties.getProperty(TAG_RECONNECT_POLICY,
            ExponentialReconnectionPolicy.class.getSimpleName() + "()");
    }

    /**
     * Sets the reconnection policy.
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
     * @param policy The reconnection policy.
     */
    public void setReconnectionPolicy(final String policy) {
        this.setDataSourceProperty(TAG_RECONNECT_POLICY, policy);
    }

    /**
     * Gets the default fetch size for all the queries returning result sets.
     * <p>
     *     This value is the number of rows the server will return in each network frame. It corresponds to the
     *     property {@code datastax-java-driver.basic.request.page-size}. See the documentation about the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/paging/">paging</a> for
     *     further details.
     *     The default value is {@value CassandraConnection#FALLBACK_FETCH_SIZE}.
     * </p>
     *
     * @return The fetch size.
     */
    public Integer getFetchSize() {
        return (Integer) this.properties.getOrDefault(TAG_FETCH_SIZE, FALLBACK_FETCH_SIZE);
    }

    /**
     * Sets the default fetch size for all the queries returning result sets.
     * <p>
     *     This value is the number of rows the server will return in each network frame. It corresponds to the
     *     property {@code datastax-java-driver.basic.request.page-size}. See the documentation about the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/paging/">paging</a> for
     *     further details.
     * </p>
     *
     * @param fetchSize The fetch size.
     */
    public void setFetchSize(final Integer fetchSize) {
        this.setDataSourceProperty(TAG_FETCH_SIZE, fetchSize);
    }

    /**
     * Gets whether the secured traffic is enabled for the created connection.
     *
     * @return {@code true} if the secured traffic is enabled, {@code false} otherwise.
     */
    public boolean isSslEnabled() {
        return (boolean) this.properties.getOrDefault(TAG_ENABLE_SSL, false);
    }

    /**
     * Sets whether the secured traffic is enabled for the created connection.
     * <p>
     *     When enabled, the default SSL engine {@link DefaultSslEngineFactory} is used with the JSSE system properties
     *     defined for your application.
     * </p>
     *
     * @param enabled Whether the secured traffic is enabled.
     */
    public void setSslEnabled(final boolean enabled) {
        this.setDataSourceProperty(TAG_ENABLE_SSL, enabled);
    }

    /**
     * Gets the SSL engine factory.
     * <p>
     *     The default value is {@link DefaultSslEngineFactory}.
     * </p>
     *
     * @return The SSL engine factory.
     */
    public String getSslEngineFactory() {
        return this.properties.getProperty(TAG_SSL_ENGINE_FACTORY, DefaultSslEngineFactory.class.getSimpleName());
    }

    /**
     * Sets the SSL engine factory.
     * <p>
     *     The value must be the fully-qualified name of a class with a no-args constructor implementing
     *     {@link SslEngineFactory} interface.
     * </p>
     *
     * @param factory The SSL engine factory.
     */
    public void setSslEngineFactory(final String factory) {
        this.setDataSourceProperty(TAG_SSL_ENGINE_FACTORY, factory);
    }

    /**
     * Gets whether the validation of the server certificate's common name against the hostname of the server being
     * connected to is enabled.
     * <p>
     *     The default value is {@code true} when the {@link DefaultSslEngineFactory} is used.
     * </p>
     *
     * @return {@code true} if the hostname verification is enabled, {@code false} otherwise.
     */
    public boolean isHostnameVerified() {
        return (boolean) this.properties.getOrDefault(TAG_SSL_HOSTNAME_VERIFICATION,
            isSslEnabled() && getSslEngineFactory().contains(DefaultSslEngineFactory.class.getSimpleName()));
    }

    /**
     * Sets whether the validation of the server certificate's common name against the hostname of the server being
     * connected to is enabled.
     *
     * @param enabled Whether the hostname verification is enabled.
     */
    public void setHostnameVerified(final boolean enabled) {
        this.setDataSourceProperty(TAG_SSL_HOSTNAME_VERIFICATION, enabled);
    }

    /**
     * Gets the fully qualified path of the cloud secure connect bundle file used to connect to an AstraDB instance.
     *
     * @return The fully qualified path of the cloud secure connect bundle file.
     */
    public String getSecureConnectBundle() {
        return this.properties.getProperty(TAG_CLOUD_SECURE_CONNECT_BUNDLE);
    }

    /**
     * Sets the fully qualified path of the cloud secure connect bundle file used to connect to an AstraDB instance.
     *
     * @param bundlePath The fully qualified path of the cloud secure connect bundle file.
     */
    public void setSecureConnectBundle(final String bundlePath) {
        this.setDataSourceProperty(TAG_CLOUD_SECURE_CONNECT_BUNDLE, bundlePath);
    }

    /**
     * Sets the path of the cloud secure connect bundle file used to connect to an AstraDB instance.
     *
     * @param bundlePath The path of the cloud secure connect bundle file.
     */
    public void setSecureConnectBundle(final Path bundlePath) {
        this.setSecureConnectBundle(bundlePath.toString());
    }

    /**
     * Gets the token used to connect to an AstraDB instance.
     *
     * @return The token.
     */
    public String getToken() {
        return this.properties.getProperty(TAG_TOKEN);
    }

    /**
     * Sets the token used to connect to an AstraDB instance.
     *
     * @param token The token.
     */
    public void setToken(final String token) {
        this.setDataSourceProperty(TAG_TOKEN, token);
    }

    /**
     * Gets the region of the AstraDB instance.
     *
     * @return The AstraDB region.
     */
    public String getAstraRegion() {
        return this.properties.getProperty(TAG_ASTRA_REGION);
    }

    /**
     * Sets the region of the AstraDB instance.
     *
     * @param region The AstraDB region.
     */
    public void setAstraRegion(final String region) {
        this.setDataSourceProperty(TAG_ASTRA_REGION, region);
    }

    /**
     * Gets whether the Kerberos auth provider is enabled.
     * <p>
     *     The default value is {@code false}.
     *     See <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/authentication/">
     *     Authentication reference</a> and <a href="https://github.com/instaclustr/cassandra-java-driver-kerberos">
     *     Kerberos authenticator for Java driver</a> for further information.
     * </p>
     *
     * @return {@code true} if the Kerberos auth provider is enabled, {@code false} otherwise.
     */
    public boolean isKerberosAuthProviderEnabled() {
        return (boolean) this.properties.getOrDefault(TAG_USE_KERBEROS, false);
    }

    /**
     * Sets whether the Kerberos auth provider is enabled.
     * <p>
     *     This will enable the Kerberos {@link AuthProvider} implementation for the connection with default parameters.
     *     See <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/authentication/">
     *     Authentication reference</a> and <a href="https://github.com/instaclustr/cassandra-java-driver-kerberos">
     *     Kerberos authenticator for Java driver</a> for further information.
     * </p>
     *
     * @param enabled Whether the Kerberos auth provider is enabled.
     */
    public void setKerberosAuthProviderEnabled(final boolean enabled) {
        this.setDataSourceProperty(TAG_USE_KERBEROS, enabled);
    }

    /**
     * Gets the request timeout in milliseconds.
     * <p>
     *     It corresponds to the property {@code basic.request.timeout} in the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference">
     *     Configuration reference page</a>.
     *     The default value is 2 seconds.
     * </p>
     *
     * @return The request timeout in milliseconds.
     */
    public Long getRequestTimeout() {
        return (Long) this.properties.getOrDefault(TAG_REQUEST_TIMEOUT, 2_000L);
    }

    /**
     * Sets the request timeout in milliseconds.
     * <p>
     *     It corresponds to the property {@code basic.request.timeout} in the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference">
     *     Configuration reference page</a>.
     * </p>
     *
     * @param timeout The request timeout in milliseconds.
     */
    public void setRequestTimeout(final Long timeout) {
        this.setDataSourceProperty(TAG_REQUEST_TIMEOUT, timeout);
    }

    /**
     * Gets the connection timeout in milliseconds.
     * <p>
     *     It corresponds to the property {@code advanced.connection.connect-timeout} in the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference">
     *     Configuration reference page</a>.
     *     The default value is 5 seconds.
     * </p>
     *
     * @return The connection timeout in milliseconds.
     */
    public Long getConnectionTimeout() {
        return (Long) this.properties.getOrDefault(TAG_CONNECT_TIMEOUT, 5_000L);
    }

    /**
     * Sets the connection timeout in milliseconds.
     * <p>
     *     It corresponds to the property {@code advanced.connection.connect-timeout} in the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference">
     *     Configuration reference page</a>.
     * </p>
     *
     * @param timeout The connection timeout in milliseconds.
     */
    public void setConnectionTimeout(final Long timeout) {
        this.setDataSourceProperty(TAG_CONNECT_TIMEOUT, timeout);
    }

    /**
     * Gets whether the <a href="https://en.wikipedia.org/wiki/Nagle%27s_algorithm">Nagle's algorithm</a> is enabled.
     * <p>
     *     It corresponds to the property {@code advanced.socket.tcp-no-delay} in the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference">
     *     Configuration reference page</a>.
     *     The default value is {@code true}.
     * </p>
     *
     * @return {@code true} if the Nagle's algorithm is enabled, {@code false} otherwise.
     */
    public boolean isTcpNoDelayEnabled() {
        return (boolean) this.properties.getOrDefault(TAG_TCP_NO_DELAY, true);
    }

    /**
     * Sets whether the <a href="https://en.wikipedia.org/wiki/Nagle%27s_algorithm">Nagle's algorithm</a> is enabled.
     * <p>
     *     It corresponds to the property {@code advanced.socket.tcp-no-delay} in the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference">
     *     Configuration reference page</a>.
     * </p>
     *
     * @param enabled Whether the Nagle's algorithm is enabled.
     */
    public void setTcpNoDelayEnabled(final boolean enabled) {
        this.setDataSourceProperty(TAG_TCP_NO_DELAY, enabled);
    }

    /**
     * Gets whether the TCP keep-alive is enabled.
     * <p>
     *     It corresponds to the property {@code advanced.socket.keep-alive} in the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference">
     *     Configuration reference page</a>.
     *     The default value is {@code false}.
     * </p>
     *
     * @return {@code true} if the TCP keep-alive is enabled, {@code false} otherwise.
     */
    public boolean isTcpKeepAliveEnabled() {
        return (boolean) this.properties.getOrDefault(TAG_KEEP_ALIVE, false);
    }

    /**
     * Sets whether the TCP keep-alive is enabled.
     * <p>
     *     It corresponds to the property {@code advanced.socket.keep-alive} in the
     *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference">
     *     Configuration reference page</a>.
     * </p>
     *
     * @param enabled Whether the TCP keep-alive is enabled.
     */
    public void setTcpKeepAliveEnabled(final boolean enabled) {
        this.setDataSourceProperty(TAG_KEEP_ALIVE, enabled);
    }

    /**
     * Gets the fully qualified path of the
     * <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference/">
     * client configuration file</a> used for the created connection.
     *
     * @return The fully qualified path of the client configuration file.
     */
    public String getConfigurationFile() {
        return this.properties.getProperty(TAG_CONFIG_FILE);
    }

    /**
     * Sets the fully qualified path of the
     * <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference/">
     * client configuration file</a> used for the created connection.
     *
     * @param configurationFile The fully qualified path of the client configuration file.
     */
    public void setConfigurationFile(final String configurationFile) {
        this.setDataSourceProperty(TAG_CONFIG_FILE, configurationFile);
    }

    /**
     * Sets the path of the
     * <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference/">
     * client configuration file</a>used for the created connection.
     *
     * @param configurationFilePath The path of the client configuration file.
     */
    public void setConfigurationFile(final Path configurationFilePath) {
        this.setConfigurationFile(configurationFilePath.toString());
    }

    /**
     * Gets the AWS region of the contact point of the Amazon Keyspaces instance.
     *
     * @return The AWS region.
     */
    public String getAwsRegion() {
        return this.properties.getProperty(TAG_AWS_REGION);
    }

    /**
     * Sets the AWS region of the contact point of the Amazon Keyspaces instance.
     *
     * @param region The string representation of the region.
     */
    public void setAwsRegion(final String region) {
        this.setDataSourceProperty(TAG_AWS_REGION, region);
    }

    /**
     * Sets the AWS region of the contact point of the Amazon Keyspaces instance.
     *
     * @param region The AWS region.
     */
    public void setAwsRegion(final Region region) {
        if (region == null) {
            this.setAwsRegion((String) null);
        } else {
            this.setDataSourceProperty(TAG_AWS_REGION, region.id());
        }
    }

    /**
     * Gets the AWS region of the Amazon Secret Manager in which the credentials of the user used for the connection
     * are stored. If not defined, the value is the one returned by {@link #getAwsRegion()}.
     *
     * @return .
     */
    public String getAwsSecretRegion() {
        return (String) this.properties.getOrDefault(TAG_AWS_SECRET_REGION,
            this.properties.getProperty(TAG_AWS_REGION));
    }

    /**
     * Sets the AWS region of the Amazon Secret Manager in which the credentials of the user used for the connection
     * are stored.
     *
     * @param region The string representation of the region.
     */
    public void setAwsSecretRegion(final String region) {
        this.setDataSourceProperty(TAG_AWS_SECRET_REGION, region);
    }

    /**
     * Sets the AWS region of the Amazon Secret Manager in which the credentials of the user used for the connection
     * are stored.
     *
     * @param region The AWS region.
     */
    public void setAwsSecretRegion(final Region region) {
        if (region == null) {
            this.setAwsSecretRegion((String) null);
        } else {
            this.setDataSourceProperty(TAG_AWS_SECRET_REGION, region.id());
        }
    }

    /**
     * Gets the name of the secret, stored in Amazon Secret Manager, containing the credentials of the user used for
     * the connection.
     *
     * @return The name of the secret.
     */
    public String getAwsSecretName() {
        return this.properties.getProperty(TAG_AWS_SECRET_NAME);
    }

    /**
     * Sets the name of the secret, stored in Amazon Secret Manager, containing the credentials of the user used for
     * the connection.
     *
     * @param secretName The name of the secret.
     */
    public void setAwsSecretName(final String secretName) {
        this.setDataSourceProperty(TAG_AWS_SECRET_NAME, secretName);
    }

    /**
     * Gets whether the Amazon Signature V4 auth provider is enabled.
     * <p>
     *     The default value is {@code false}.
     *     See <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/authentication/">
     *     Authentication reference</a> and
     *     <a href="https://github.com/aws/aws-sigv4-auth-cassandra-java-driver-plugin">
     *     Amazon Signature V4 authenticator plugin for Java driver</a> for further information.
     * </p>
     *
     * @return {@code true} if the Amazon Signature V4 auth provider is enabled, {@code false} otherwise.
     */
    public boolean isSigV4AuthProviderEnabled() {
        return (boolean) this.properties.getOrDefault(TAG_USE_SIG_V4, false);
    }

    /**
     * Sets whether the Amazon Signature V4 auth provider is enabled.
     * <p>
     *     This will enable the Amazon Signature V4 {@link AuthProvider} implementation for the connection using the
     *     AWS region defined in the property {@link #setAwsRegion(String)} (or {@link #setAwsRegion(Region)}).
     *     See <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/authentication/">
     *     Authentication reference</a> and
     *     <a href="https://github.com/aws/aws-sigv4-auth-cassandra-java-driver-plugin">
     *     Amazon Signature V4 authenticator plugin for Java driver</a> for further information.
     * </p>
     *
     * @param enabled Whether the Amazon Signature V4 auth provider is enabled.
     */
    public void setSigV4AuthProviderEnabled(final boolean enabled) {
        this.setDataSourceProperty(TAG_USE_SIG_V4, enabled);
    }

    private void setDataSourceProperty(final String propertyName, final Object value) {
        if (value == null) {
            this.properties.remove(propertyName);
        } else {
            this.properties.put(propertyName, value);
        }
    }

    private Properties convertAllPropertiesToString() {
        this.properties.forEach((propertyName, value) -> {
            if (Arrays.asList(Integer.class, Long.class, Boolean.class).contains(value.getClass())) {
                // Need to convert non-String values to be supported by SessionHolder when creating the session,
                // otherwise such values are ignored and generate NullPointerExceptions (see issue #80).
                this.properties.put(propertyName, value.toString());
            }
        });
        return this.properties;
    }
}
