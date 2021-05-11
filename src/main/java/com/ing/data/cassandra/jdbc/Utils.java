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

/**
 * A set of static utility methods and constants used by the JDBC wrapper, various default values and error message
 * strings that can be shared across classes.
 */
public final class Utils {
    /**
     * JDBC protocol for Cassandra connection.
     */
    public static final String PROTOCOL = "jdbc:cassandra:";
    /**
     * Default Cassandra cluster port.
     */
    public static final int DEFAULT_PORT = 9042;

    /**
     * JDBC URL parameter key for the database version.
     */
    public static final String KEY_VERSION = "version";
    /**
     * JDBC URL parameter key for the consistency.
     */
    public static final String KEY_CONSISTENCY = "consistency";
    /**
     * JDBC URL parameter key for the primary data center.
     */
    public static final String KEY_PRIMARY_DC = "primarydc";
    /**
     * JDBC URL parameter key for the backup data center.
     */
    public static final String KEY_BACKUP_DC = "backupdc";
    /**
     * JDBC URL parameter key for the connection number of retries.
     */
    public static final String KEY_CONNECTION_RETRIES = "retries";
    /**
     * JDBC URL parameter key for the load balancing policy.
     */
    public static final String KEY_LOAD_BALANCING_POLICY = "loadbalancing";
    /**
     * JDBC URL parameter key for the local data center.
     */
    public static final String KEY_LOCAL_DATACENTER = "localdatacenter";
    /**
     * JDBC URL parameter key for the retry policy.
     */
    public static final String KEY_RETRY_POLICY = "retry";
    /**
     * JDBC URL parameter key for the reconnection policy.
     */
    public static final String KEY_RECONNECT_POLICY = "reconnection";
    /**
     * JDBC URL parameter key for the debug mode.
     */
    public static final String KEY_DEBUG = "debug";
    /**
     * JDBC URL parameter key for SSL enabling.
     */
    public static final String KEY_ENABLE_SSL = "enablessl";
    /**
     * JDBC URL parameter key for the custom SSL engine factory ({@link SslEngineFactory}).
     */
    public static final String KEY_SSL_ENGINE_FACTORY = "sslenginefactory";
    /**
     * JDBC URL parameter key for the cloud secure connect bundle.
     */
    public static final String KEY_CLOUD_SECURE_CONNECT_BUNDLE = "secureconnectbundle";

    public static final String TAG_USER = "user";
    public static final String TAG_PASSWORD = "password";
    public static final String TAG_DATABASE_NAME = "databaseName";
    public static final String TAG_SERVER_NAME = "serverName";
    public static final String TAG_PORT_NUMBER = "portNumber";
    public static final String TAG_ACTIVE_CQL_VERSION = "activeCqlVersion";
    public static final String TAG_CQL_VERSION = "cqlVersion";
    public static final String TAG_CONSISTENCY_LEVEL = "consistencyLevel";
    public static final String TAG_LOAD_BALANCING_POLICY = "loadBalancing";
    public static final String TAG_LOCAL_DATACENTER = "localDatacenter";
    public static final String TAG_RETRY_POLICY = "retry";
    public static final String TAG_RECONNECT_POLICY = "reconnection";
    public static final String TAG_DEBUG = "debug";
    public static final String TAG_PRIMARY_DC = "primaryDatacenter";
    public static final String TAG_BACKUP_DC = "backupDatacenter";
    public static final String TAG_CONNECTION_RETRIES = "retries";
    public static final String TAG_ENABLE_SSL = "enableSsl";
    public static final String TAG_SSL_ENGINE_FACTORY = "sslEngineFactory";
    public static final String TAG_CLOUD_SECURE_CONNECT_BUNDLE = "secureConnectBundle";

    public static final String JSSE_TRUSTSTORE_PROPERTY = "javax.net.ssl.trustStore";
    public static final String JSSE_TRUSTSTORE_PASSWORD_PROPERTY = "javax.net.ssl.trustStorePassword";
    public static final String JSSE_KEYSTORE_PROPERTY = "javax.net.ssl.keyStore";
    public static final String JSSE_KEYSTORE_PASSWORD_PROPERTY = "javax.net.ssl.keyStorePassword";

    /**
     * {@code NULL} CQL keyword.
     */
    public static final String NULL_KEYWORD = "NULL";

    protected static final String WAS_CLOSED_CONN = "Method was called on a closed Connection.";
    protected static final String WAS_CLOSED_STMT = "Method was called on a closed Statement.";
    protected static final String WAS_CLOSED_RS = "Method was called on a closed ResultSet.";
    protected static final String NO_INTERFACE = "No object was found that matched the provided interface: %s";
    protected static final String NO_TRANSACTIONS = "The Cassandra implementation does not support transactions.";
    protected static final String ALWAYS_AUTOCOMMIT = "The Cassandra implementation is always in auto-commit mode.";
    protected static final String BAD_TIMEOUT = "The timeout value was less than zero.";
    protected static final String NOT_SUPPORTED = "The Cassandra implementation does not support this method.";
    protected static final String NO_GEN_KEYS =
        "The Cassandra implementation does not currently support returning generated keys.";
    protected static final String NO_MULTIPLE =
        "The Cassandra implementation does not currently support multiple open Result Sets.";
    protected static final String NO_RESULT_SET =
        "No ResultSet returned from the CQL statement passed in an 'executeQuery()' method.";
    protected static final String BAD_KEEP_RS =
        "The argument for keeping the current result set: %s is not a valid value.";
    protected static final String BAD_TYPE_RS = "The argument for result set type: %s is not a valid value.";
    protected static final String BAD_CONCURRENCY_RS =
        "The argument for result set concurrency: %s is not a valid value.";
    protected static final String BAD_HOLD_RS =
        "The argument for result set holdability: %s is not a valid value.";
    protected static final String BAD_FETCH_DIR = "Fetch direction value of: %s is illegal.";
    protected static final String BAD_AUTO_GEN = "Auto key generation value of: %s is illegal.";
    protected static final String BAD_FETCH_SIZE = "Fetch size of: %s rows may not be negative.";
    protected static final String MUST_BE_POSITIVE =
        "Index must be a positive number less or equal the count of returned columns: %d";
    protected static final String VALID_LABELS = "Name provided was not in the list of valid column labels: %s";
    protected static final String HOST_IN_URL =
        "Connection url must specify a host, e.g. jdbc:cassandra://localhost:9042/keyspace";
    protected static final String HOST_REQUIRED = "A 'host' name is required to build a Connection.";
    protected static final String BAD_KEYSPACE =
        "Keyspace names must be composed of alphanumerics and underscores (parsed: '%s').";
    protected static final String URI_IS_SIMPLE =
        "Connection url may only include host, port, and keyspace, consistency and version option, e.g. "
            + "jdbc:cassandra://localhost:9042/keyspace?version=3.0.0&consistency=ONE";
    protected static final String FORWARD_ONLY = "Can not position cursor with a type of TYPE_FORWARD_ONLY.";
    protected static final String MALFORMED_URL = "The string '%s' is not a valid URL.";
    protected static final String SSL_CONFIG_FAILED = "Unable to configure SSL: %s.";

    static final Logger log = LoggerFactory.getLogger(Utils.class);

    private Utils() {
        // Private constructor to hide the public one.
    }

    /**
     * Parses a URL for the Cassandra JDBC Driver.
     * <p>
     *     The URL must start with the protocol: {@value #PROTOCOL}.
     *     The URI part (the "sub-name") must contain a host, an optional port and optional keyspace name, for example:
     *     "//localhost:9160/Test1".
     * </p>
     *
     * @param url The full JDBC URL to be parsed.
     * @return A list of properties that were parsed from the "subname".
     * @throws SQLException when something went wrong during the URL parsing.
     */
    public static Properties parseURL(final String url) throws SQLException {
        final Properties props = new Properties();

        if (url != null) {
            props.setProperty(TAG_PORT_NUMBER, String.valueOf(DEFAULT_PORT));
            final String rawUri = url.substring(PROTOCOL.length());
            final URI uri;
            try {
                uri = new URI(rawUri);
            } catch (final URISyntaxException e) {
                throw new SQLSyntaxErrorException(e);
            }

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
            if ((query != null) && (!query.isEmpty())) {
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
                if (params.containsKey(KEY_PRIMARY_DC)) {
                    props.setProperty(TAG_PRIMARY_DC, params.get(KEY_PRIMARY_DC));
                }
                if (params.containsKey(KEY_BACKUP_DC)) {
                    props.setProperty(TAG_BACKUP_DC, params.get(KEY_BACKUP_DC));
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
                if (params.containsKey(KEY_CLOUD_SECURE_CONNECT_BUNDLE)) {
                    props.setProperty(TAG_CLOUD_SECURE_CONNECT_BUNDLE, params.get(KEY_CLOUD_SECURE_CONNECT_BUNDLE));
                }
                if (params.containsKey(TAG_USER)) {
                    props.setProperty(TAG_USER, params.get(TAG_USER));
                }
                if (params.containsKey(TAG_PASSWORD)) {
                    props.setProperty(TAG_PASSWORD, params.get(TAG_PASSWORD));
                }
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("URL: '{}' parsed to: {}", url, props);
        }

        return props;
    }

    /**
     * Creates a "sub-name" portion of a JDBC URL from properties.
     *
     * @param props A {@link Properties} instance containing all the properties to be considered.
     * @return A "sub-name" portion of a JDBC URL (for example: //myhost:9160/Test1?version=3.0.0).
     * @throws SQLException when something went wrong during the "sub-name" creation.
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

        if (log.isTraceEnabled()) {
            log.trace("Sub-name: '{}' created from: {}", uri.toString(), props);
        }

        return uri.toString();
    }

    /**
     * Builds the URI part containing the query parameters "consistency" and "version" from properties.
     *
     * @param props A {@link Properties} instance containing all the properties to be considered.
     * @return The URI part containing the query parameters (for example: "consistency=ONE&amp;version=3.0.0") or
     * {@code null} if neither version nor consistency are defined in the provided properties.
     */
    protected static String makeQueryString(final Properties props) {
        final StringBuilder sb = new StringBuilder();
        final String version = (props.getProperty(TAG_CQL_VERSION));
        final String consistency = (props.getProperty(TAG_CONSISTENCY_LEVEL));
        if (StringUtils.isNotBlank(consistency)) {
            sb.append(KEY_CONSISTENCY).append("=").append(consistency);
        }
        if (StringUtils.isNotBlank(version)) {
            if (sb.length() != 0) {
                sb.append("&");
            }
            sb.append(KEY_VERSION).append("=").append(version);
        }

        if (sb.length() > 0) {
            return sb.toString().trim();
        } else {
            return null;
        }
    }

    /**
     * Parses the query parameters from a the query part of a JDBC URL.
     *
     * @param query The query part of the JDBC URL.
     * @return The map of the parsed parameters.
     * @throws SQLException when something went wrong during the parsing.
     */
    protected static Map<String, String> parseQueryPart(final String query) throws SQLException {
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
     * @return A map of {@link DriverOption} values parsed from the givne string.
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

    private static Map<DriverOption, Object> getReconnectionPolicy(String primaryReconnectionPolicy,
                                                                   final String parameters) {
        final Map<DriverOption, Object> policyParametersMap = new HashMap<>();
        if (!primaryReconnectionPolicy.contains(".")) {
            primaryReconnectionPolicy = "com.datastax.oss.driver.internal.core.connection." + primaryReconnectionPolicy;
        }

        policyParametersMap.put(DefaultDriverOption.RECONNECTION_POLICY_CLASS, primaryReconnectionPolicy);

        // Parameters have been specified
        if (parameters.length() > 0) {
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
                            } else if (argPos == 1 &&
                                "com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy".equals(
                                    primaryReconnectionPolicy)) {
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
