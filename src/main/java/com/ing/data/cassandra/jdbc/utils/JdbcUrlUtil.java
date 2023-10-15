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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_KEYSPACE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.HOST_IN_URL;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.HOST_REQUIRED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.SECURECONENCTBUNDLE_REQUIRED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.URI_IS_SIMPLE;

/**
 * A set of static utility methods and constants used to parse the JDBC URL used to establish a connection to a
 * Cassandra database.
 */
public final class JdbcUrlUtil {

    /**
     * Default Cassandra cluster port.
     */
    public static final int DEFAULT_PORT = 9042;

    /**
     * JDBC protocol for Cassandra connection.
     */
    public static final String PROTOCOL = "jdbc:cassandra:";

    /**
     * JDBC protocol for Cassandra DBaaS connection.
     */
    public static final String PROTOCOL_DBAAS = "jdbc:cassandra:dbaas:";

    /**
     * JDBC URL parameter key for the CQL version.
     * @deprecated For removal.
     */
    @Deprecated
    @SuppressWarnings("DeprecatedIsStillUsed")
    public static final String KEY_VERSION = "version";

    /**
     * Property name used to retrieve the active CQL version when the connection to Cassandra is established. This
     * property is mapped from the JDBC URL parameter {@code version} or from the default value defined in the
     * property {@code database.defaultCqlVersion} of the resource file 'jdbc-driver.properties'.
     * @deprecated For removal, because {@link #KEY_VERSION} and {@link #TAG_CQL_VERSION} are deprecated.
     */
    @Deprecated
    @SuppressWarnings("DeprecatedIsStillUsed")
    public static final String TAG_ACTIVE_CQL_VERSION = "activeCqlVersion";

    /**
     * Property name used to retrieve the active CQL version when the connection to Cassandra is established. This
     * property is mapped from the JDBC URL parameter {@code version}.
     * @deprecated For removal.
     */
    @Deprecated
    @SuppressWarnings("DeprecatedIsStillUsed")
    public static final String TAG_CQL_VERSION = "cqlVersion";

    /**
     * JDBC URL parameter key for the consistency.
     */
    public static final String KEY_CONSISTENCY = "consistency";

    /**
     * Property name used to retrieve the consistency when the connection to Cassandra is established. This property
     * is mapped from the JDBC URL parameter {@code consistency}.
     */
    public static final String TAG_CONSISTENCY_LEVEL = "consistencyLevel";

    /**
     * JDBC URL parameter key for the connection number of retries.
     */
    public static final String KEY_CONNECTION_RETRIES = "retries";

    /**
     * Property name used to retrieve the number of retries when the connection to Cassandra is established. This
     * property is mapped from the JDBC URL parameter {@code retries}.
     */
    public static final String TAG_CONNECTION_RETRIES = "retries";

    /**
     * JDBC URL parameter key for the load balancing policy.
     */
    public static final String KEY_LOAD_BALANCING_POLICY = "loadbalancing";

    /**
     * Property name used to retrieve the load balancing policy when the connection to Cassandra is established. This
     * property is mapped from the JDBC URL parameter {@code loadbalancing}.
     */
    public static final String TAG_LOAD_BALANCING_POLICY = "loadBalancing";

    /**
     * JDBC URL parameter key for the local data center.
     */
    public static final String KEY_LOCAL_DATACENTER = "localdatacenter";

    /**
     * Property name used to retrieve the local data center when the connection to Cassandra is established. This
     * property is mapped from the JDBC URL parameter {@code localdatacenter}.
     */
    public static final String TAG_LOCAL_DATACENTER = "localDatacenter";

    /**
     * JDBC URL parameter key for the retry policy.
     */
    public static final String KEY_RETRY_POLICY = "retry";

    /**
     * Property name used to retrieve the retry policy when the connection to Cassandra is established. This
     * property is mapped from the JDBC URL parameter {@code retry}.
     */
    public static final String TAG_RETRY_POLICY = "retry";

    /**
     * JDBC URL parameter key for the reconnection policy.
     */
    public static final String KEY_RECONNECT_POLICY = "reconnection";

    /**
     * Property name used to retrieve the reconnection policy when the connection to Cassandra is established. This
     * property is mapped from the JDBC URL parameter {@code reconnection}.
     */
    public static final String TAG_RECONNECT_POLICY = "reconnection";

    /**
     * JDBC URL parameter key for the debug mode.
     */
    public static final String KEY_DEBUG = "debug";

    /**
     * Property name used to retrieve the debug mode value when the connection to Cassandra is established. This
     * property is mapped from the JDBC URL parameter {@code debug}.
     */
    public static final String TAG_DEBUG = "debug";

    /**
     * JDBC URL parameter key for SSL enabling.
     */
    public static final String KEY_ENABLE_SSL = "enablessl";

    /**
     * Property name used to retrieve the SSL enabling value when the connection to Cassandra is established. This
     * property is mapped from the JDBC URL parameter {@code enablessl}.
     */
    public static final String TAG_ENABLE_SSL = "enableSsl";

    /**
     * JDBC URL parameter key for the custom SSL engine factory ({@link SslEngineFactory}).
     */
    public static final String KEY_SSL_ENGINE_FACTORY = "sslenginefactory";

    /**
     * Property name used to retrieve the custom SSL engine factory when the connection to Cassandra is established.
     * This property is mapped from the JDBC URL parameter {@code sslenginefactory}.
     */
    public static final String TAG_SSL_ENGINE_FACTORY = "sslEngineFactory";

    /**
     * JDBC URL parameter key for SSL hostname verification disabling.
     */
    public static final String KEY_SSL_HOSTNAME_VERIFICATION = "hostnameverification";

    /**
     * Property name used to retrieve the SSL hostname verification enabling when the connection to Cassandra is
     * established. This property is mapped from the JDBC URL parameter {@code hostnameverification}.
     */
    public static final String TAG_SSL_HOSTNAME_VERIFICATION = "hostnameVerification";

    /**
     * JDBC URL parameter key for the cloud secure connect bundle.
     */
    public static final String KEY_CLOUD_SECURE_CONNECT_BUNDLE = "secureconnectbundle";

    /**
     * Property name used to retrieve the secure connect Bundle when the connection to Cassandra DBaaS is established.
     * This property is mapped from the JDBC URL parameter {@code secureconnectbundle}.
     */
    public static final String TAG_CLOUD_SECURE_CONNECT_BUNDLE = "secureConnectBundle";

    /**
     * JDBC URL parameter key for the username.
     */
    public static final String KEY_USER = "user";

    /**
     * Property name used to retrieve the username when the connection to Cassandra is established. This property
     * is mapped from the JDBC URL parameter {@code user}.
     */
    public static final String TAG_USER = "user";

    /**
     * JDBC URL parameter key for the user password.
     */
    public static final String KEY_PASSWORD = "password";

    /**
     * Property name used to retrieve the user password when the connection to Cassandra is established. This property
     * is mapped from the JDBC URL parameter {@code password}.
     */
    public static final String TAG_PASSWORD = "password";

    /**
     * JDBC URL parameter key for the request timeout.
     */
    public static final String KEY_REQUEST_TIMEOUT = "requesttimeout";

    /**
     * Property name used to retrieve the request timeout when the connection to Cassandra is established. This property
     * is mapped from the JDBC URL parameter {@code requesttimeout}.
     */
    public static final String TAG_REQUEST_TIMEOUT = "requestTimeout";

    /**
     * JDBC URL parameter key for the connection timeout.
     */
    public static final String KEY_CONNECT_TIMEOUT = "connecttimeout";

    /**
     * Property name used to retrieve the connection timeout when the connection to Cassandra is established. This
     * property is mapped from the JDBC URL parameter {@code connecttimeout}.
     */
    public static final String TAG_CONNECT_TIMEOUT = "connectTimeout";

    /**
     * JDBC URL parameter key for the Nagle's algorithm enabling.
     */
    public static final String KEY_TCP_NO_DELAY = "tcpnodelay";

    /**
     * Property name used to retrieve the Nagle's algorithm enabling when the connection to Cassandra is established.
     * This property is mapped from the JDBC URL parameter {@code tcpnodelay}.
     */
    public static final String TAG_TCP_NO_DELAY = "tcpNoDelay";

    /**
     * JDBC URL parameter key for the TCP keep-alive enabling.
     */
    public static final String KEY_KEEP_ALIVE = "keepalive";

    /**
     * Property name used to retrieve the TCP keep-alive enabling when the connection to Cassandra is established.
     * This property is mapped from the JDBC URL parameter {@code keepalive}.
     */
    public static final String TAG_KEEP_ALIVE = "keepAlive";

    /**
     * JDBC URL parameter key for the configuration file.
     */
    public static final String KEY_CONFIG_FILE = "configfile";

    /**
     * Property name used to retrieve the configuration file when the connection to Cassandra is established.
     * This property is mapped from the JDBC URL parameter {@code configfile}.
     */
    public static final String TAG_CONFIG_FILE = "configFile";

    /**
     * JDBC URL parameter key for the compliance mode.
     */
    public static final String KEY_COMPLIANCE_MODE = "compliancemode";

    /**
     * Property name used to retrieve the compliance mode to use when the connection to Cassandra is established.
     * This property is mapped from the JDBC URL parameter {@code compliancemode}.
     */
    public static final String TAG_COMPLIANCE_MODE = "complianceMode";

    /**
     * Property name used to retrieve the keyspace name when the connection to Cassandra is established. This property
     * is mapped from the JDBC URL keyspace path parameter.
     */
    public static final String TAG_DATABASE_NAME = "databaseName";

    /**
     * Property name used to retrieve the contact points when the connection to Cassandra is established. This property
     * is mapped from the JDBC URL host.
     */
    public static final String TAG_SERVER_NAME = "serverName";

    /**
     * Property name used to retrieve the port used when the connection to Cassandra is established. This property
     * is mapped from the JDBC URL port.
     */
    public static final String TAG_PORT_NUMBER = "portNumber";

    static final Logger LOG = LoggerFactory.getLogger(JdbcUrlUtil.class);

    private JdbcUrlUtil() {
        // Private constructor to hide the public one.
    }

    /**
     * Parses a URL for the Cassandra JDBC Driver.
     * <p>
     *     The URL must start with the protocol {@value #PROTOCOL} or {@value #PROTOCOL_DBAAS} for a connection to a
     *     cloud database.
     *     The URI part (the "sub-name") must contain a host, an optional port and optional keyspace name, for example:
     *     "//localhost:9160/Test1", except for a connection to a cloud database, in this case, a simple keyspace with
     *     a secure connect bundle is sufficient, for example: "///Test1?secureconnectbundle=/path/to/bundle.zip".
     * </p>
     *
     * @param url The full JDBC URL to be parsed.
     * @return A list of properties that were parsed from the "subname".
     * @throws SQLException when something went wrong during the URL parsing.
     * @throws SQLSyntaxErrorException when the URL syntax is invalid.
     * @throws SQLNonTransientConnectionException when the host is missing in the URL.
     */
    public static Properties parseURL(final String url) throws SQLException {
        final Properties props = new Properties();

        if (url != null) {
            props.setProperty(TAG_PORT_NUMBER, String.valueOf(DEFAULT_PORT));
            boolean isDbaasConnection = false;
            int uriStartIndex = PROTOCOL.length();
            if (url.startsWith(PROTOCOL_DBAAS)) {
                uriStartIndex = PROTOCOL_DBAAS.length();
                isDbaasConnection = true;
            }
            final String rawUri = url.substring(uriStartIndex);
            final URI uri;
            try {
                uri = new URI(rawUri);
            } catch (final URISyntaxException e) {
                throw new SQLSyntaxErrorException(e);
            }

            if (!isDbaasConnection) {
                final String host = uri.getHost();
                if (host == null) {
                    throw new SQLNonTransientConnectionException(HOST_IN_URL);
                }
                props.setProperty(TAG_SERVER_NAME, host);

                int port = DEFAULT_PORT;
                if (uri.getPort() >= 0) {
                    port = uri.getPort();
                }
                props.setProperty(TAG_PORT_NUMBER, String.valueOf(port));
            }

            String keyspace = uri.getPath();
            if (StringUtils.isNotEmpty(keyspace)) {
                if (keyspace.startsWith("/")) {
                    keyspace = keyspace.substring(1);
                }
                if (!keyspace.matches("[a-zA-Z]\\w+")) {
                    throw new SQLNonTransientConnectionException(String.format(BAD_KEYSPACE, keyspace));
                }
                props.setProperty(TAG_DATABASE_NAME, keyspace);
            }

            if (uri.getUserInfo() != null) {
                throw new SQLNonTransientConnectionException(URI_IS_SIMPLE);
            }

            final String query = uri.getQuery();
            if (query != null && !query.isEmpty()) {
                final Map<String, String> params = parseQueryPart(query);
                if (params.containsKey(KEY_VERSION)) {
                    props.setProperty(TAG_CQL_VERSION, params.get(KEY_VERSION));
                }
                if (params.containsKey(KEY_DEBUG)) {
                    props.setProperty(TAG_DEBUG, params.get(KEY_DEBUG));
                }
                if (params.containsKey(KEY_CONSISTENCY)) {
                    props.setProperty(TAG_CONSISTENCY_LEVEL, params.get(KEY_CONSISTENCY));
                }
                if (params.containsKey(KEY_CONNECTION_RETRIES)) {
                    props.setProperty(TAG_CONNECTION_RETRIES, params.get(KEY_CONNECTION_RETRIES));
                }
                if (params.containsKey(KEY_LOAD_BALANCING_POLICY)) {
                    props.setProperty(TAG_LOAD_BALANCING_POLICY, params.get(KEY_LOAD_BALANCING_POLICY));
                }
                if (params.containsKey(KEY_LOCAL_DATACENTER)) {
                    props.setProperty(TAG_LOCAL_DATACENTER, params.get(KEY_LOCAL_DATACENTER));
                }
                if (params.containsKey(KEY_RETRY_POLICY)) {
                    props.setProperty(TAG_RETRY_POLICY, params.get(KEY_RETRY_POLICY));
                }
                if (params.containsKey(KEY_RECONNECT_POLICY)) {
                    props.setProperty(TAG_RECONNECT_POLICY, params.get(KEY_RECONNECT_POLICY));
                }
                if (params.containsKey(KEY_ENABLE_SSL)) {
                    props.setProperty(TAG_ENABLE_SSL, params.get(KEY_ENABLE_SSL));
                }
                if (params.containsKey(KEY_SSL_ENGINE_FACTORY)) {
                    props.setProperty(TAG_SSL_ENGINE_FACTORY, params.get(KEY_SSL_ENGINE_FACTORY));
                }
                if (params.containsKey(KEY_SSL_HOSTNAME_VERIFICATION)) {
                    props.setProperty(TAG_SSL_HOSTNAME_VERIFICATION, params.get(KEY_SSL_HOSTNAME_VERIFICATION));
                }
                if (params.containsKey(KEY_CLOUD_SECURE_CONNECT_BUNDLE)) {
                    props.setProperty(TAG_CLOUD_SECURE_CONNECT_BUNDLE, params.get(KEY_CLOUD_SECURE_CONNECT_BUNDLE));
                } else if (isDbaasConnection) {
                    throw new SQLNonTransientConnectionException(SECURECONENCTBUNDLE_REQUIRED);
                }
                if (params.containsKey(KEY_USER)) {
                    props.setProperty(TAG_USER, params.get(KEY_USER));
                }
                if (params.containsKey(KEY_PASSWORD)) {
                    props.setProperty(TAG_PASSWORD, params.get(KEY_PASSWORD));
                }
                if (params.containsKey(KEY_REQUEST_TIMEOUT)) {
                    props.setProperty(TAG_REQUEST_TIMEOUT, params.get(KEY_REQUEST_TIMEOUT));
                }
                if (params.containsKey(KEY_CONNECT_TIMEOUT)) {
                    props.setProperty(TAG_CONNECT_TIMEOUT, params.get(KEY_CONNECT_TIMEOUT));
                }
                if (params.containsKey(KEY_TCP_NO_DELAY)) {
                    props.setProperty(TAG_TCP_NO_DELAY, params.get(KEY_TCP_NO_DELAY));
                }
                if (params.containsKey(KEY_KEEP_ALIVE)) {
                    props.setProperty(TAG_KEEP_ALIVE, params.get(KEY_KEEP_ALIVE));
                }
                if (params.containsKey(KEY_CONFIG_FILE)) {
                    props.setProperty(TAG_CONFIG_FILE, params.get(KEY_CONFIG_FILE));
                }
                if (params.containsKey(KEY_COMPLIANCE_MODE)) {
                    props.setProperty(TAG_COMPLIANCE_MODE, params.get(KEY_COMPLIANCE_MODE));
                }
            } else if (isDbaasConnection) {
                throw new SQLNonTransientConnectionException(SECURECONENCTBUNDLE_REQUIRED);
            }
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("URL: '{}' parsed to: {}", url, props);
        }

        return props;
    }

    /**
     * Creates a "sub-name" portion of a JDBC URL from properties.
     *
     * @param props A {@link Properties} instance containing all the properties to be considered.
     * @return A "sub-name" portion of a JDBC URL (for example: //myhost:9160/Test1?localdatacenter=DC1).
     * @throws SQLException when something went wrong during the "sub-name" creation.
     * @throws SQLNonTransientConnectionException when the host name is missing.
     */
    public static String createSubName(final Properties props) throws SQLException {
        // Make the keyspace always start with a "/" for URI.
        String keyspace = props.getProperty(TAG_DATABASE_NAME);
        if (keyspace != null) {
            keyspace = StringUtils.prependIfMissing(keyspace, "/");
        }

        final String host = props.getProperty(TAG_SERVER_NAME);
        if (host == null) {
            throw new SQLNonTransientConnectionException(HOST_REQUIRED);
        }

        // Build a valid URI from parts.
        final URI uri;
        int port = DEFAULT_PORT;
        if (StringUtils.isNotBlank(props.getProperty(TAG_PORT_NUMBER))) {
            port = Integer.parseInt(props.getProperty(TAG_PORT_NUMBER));
        }
        try {
            uri = new URI(null, null, host, port, keyspace, makeQueryString(props), null);
        } catch (final Exception e) {
            throw new SQLNonTransientConnectionException(e);
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Sub-name: '{}' created from: {}", uri, props);
        }

        return uri.toString();
    }

    /**
     * Builds the URI part containing the query parameter "consistency" from properties.
     *
     * @param props A {@link Properties} instance containing all the properties to be considered.
     * @return The URI part containing the query parameter "consistency" (for example: "consistency=ONE") or
     * {@code null} if consistency is not defined in the provided properties.
     */
    static String makeQueryString(final Properties props) {
        final StringBuilder sb = new StringBuilder();
        final String consistency = props.getProperty(TAG_CONSISTENCY_LEVEL);
        if (StringUtils.isNotBlank(consistency)) {
            sb.append(KEY_CONSISTENCY).append("=").append(consistency);
        }
        if (sb.length() > 0) {
            return sb.toString().trim();
        } else {
            return null;
        }
    }

    /**
     * Parses the query parameters from the query part of a JDBC URL.
     *
     * @param query The query part of the JDBC URL.
     * @return The map of the parsed parameters.
     * @throws SQLException when something went wrong during the parsing.
     * @throws SQLSyntaxErrorException when the encoding is not supported.
     */
    static Map<String, String> parseQueryPart(final String query) throws SQLException {
        final Map<String, String> params = new HashMap<>();
        for (final String param : query.split("&")) {
            try {
                final String[] pair = param.split("=");
                final String key = URLDecoder.decode(pair[0], StandardCharsets.UTF_8.displayName()).toLowerCase();
                String value = StringUtils.EMPTY;
                if (pair.length > 1) {
                    value = URLDecoder.decode(pair[1], StandardCharsets.UTF_8.displayName());
                }
                params.put(key, value);
            } catch (final UnsupportedEncodingException e) {
                throw new SQLSyntaxErrorException(e);
            }
        }
        return params;
    }

    /**
     * Parses the reconnection policy from a given string.
     *
     * @param reconnectionPolicyString The string containing the reconnection policy value.
     * @return A map of {@link DriverOption} values parsed from the given string.
     */
    public static Map<DriverOption, Object> parseReconnectionPolicy(final String reconnectionPolicyString) {
        final String policyRegex = "([a-zA-Z.]*Policy)(\\()(.*)(\\))";
        final Pattern policyPattern = Pattern.compile(policyRegex);
        final Matcher policyMatcher = policyPattern.matcher(reconnectionPolicyString);

        if (policyMatcher.matches()) {
            if (policyMatcher.groupCount() > 0) {
                final String primaryReconnectionPolicy = policyMatcher.group(1);
                final String reconnectionPolicyParams = policyMatcher.group(3);
                return getReconnectionPolicy(primaryReconnectionPolicy, reconnectionPolicyParams);
            }
        }

        return null;
    }

    private static Map<DriverOption, Object> getReconnectionPolicy(final String primaryReconnectionPolicy,
                                                                   final String parameters) {
        final Map<DriverOption, Object> policyParametersMap = new HashMap<>();
        String primaryReconnectionPolicyClass = primaryReconnectionPolicy;
        if (!primaryReconnectionPolicy.contains(".")) {
            primaryReconnectionPolicyClass = "com.datastax.oss.driver.internal.core.connection."
                + primaryReconnectionPolicy;
        }

        policyParametersMap.put(DefaultDriverOption.RECONNECTION_POLICY_CLASS, primaryReconnectionPolicyClass);

        // Parameters have been specified
        if (!parameters.isEmpty()) {
            final String paramsRegex = "([^,]+\\(.+?\\))|([^,]+)";
            final Pattern paramsPattern = Pattern.compile(paramsRegex);
            final Matcher paramsMatcher = paramsPattern.matcher(parameters);

            int argPos = 0;
            while (paramsMatcher.find()) {
                if (paramsMatcher.groupCount() > 0) {
                    if (paramsMatcher.group().trim().startsWith("(")) {
                        final String param = paramsMatcher.group();
                        if (param.toLowerCase().contains("(long)")) {
                            final long delay = Long.parseLong(param.toLowerCase()
                                .replace("(long)", StringUtils.EMPTY)
                                .trim());
                            if (argPos == 0) {
                                policyParametersMap.put(DefaultDriverOption.RECONNECTION_BASE_DELAY,
                                    Duration.ofSeconds(delay));
                            } else if (argPos == 1
                                && "com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy"
                                .equals(primaryReconnectionPolicyClass)) {
                                policyParametersMap.put(DefaultDriverOption.RECONNECTION_MAX_DELAY,
                                    Duration.ofSeconds(delay));
                            }
                        }
                        argPos++;
                    }
                }
            }
        }

        return policyParametersMap;
    }

}
