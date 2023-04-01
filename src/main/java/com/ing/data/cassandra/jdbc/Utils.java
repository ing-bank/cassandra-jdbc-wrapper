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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ing.data.cassandra.jdbc.json.CassandraBlobDeserializer;
import com.ing.data.cassandra.jdbc.json.CassandraBlobSerializer;
import com.ing.data.cassandra.jdbc.json.CassandraDateDeserializer;
import com.ing.data.cassandra.jdbc.json.CassandraDateTimeDeserializer;
import com.ing.data.cassandra.jdbc.json.CassandraTimeDeserializer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
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
     * JDBC protocol for Cassandra DBaaS connection.
     */
    public static final String PROTOCOL_DBAAS = "jdbc:cassandra:dbaas:";
    /**
     * Default Cassandra cluster port.
     */
    public static final int DEFAULT_PORT = 9042;
    /**
     * Properties file name containing some properties relative to this JDBC wrapper (such as JDBC driver version,
     * name, etc.).
     */
    public static final String JDBC_DRIVER_PROPERTIES_FILE = "jdbc-driver.properties";

    /**
     * JDBC URL parameter key for the database version.
     */
    public static final String KEY_VERSION = "version";
    /**
     * JDBC URL parameter key for the consistency.
     */
    public static final String KEY_CONSISTENCY = "consistency";
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
     * JDBC URL parameter key for SSL hostname verification disabling.
     */
    public static final String KEY_SSL_HOSTNAME_VERIFICATION = "hostnameverification";
    /**
     * JDBC URL parameter key for the cloud secure connect bundle.
     */
    public static final String KEY_CLOUD_SECURE_CONNECT_BUNDLE = "secureconnectbundle";
    /**
     * JDBC URL parameter key for the username.
     */
    public static final String KEY_USER = "user";
    /**
     * JDBC URL parameter key for the user password.
     */
    public static final String KEY_PASSWORD = "password";
    /**
     * JDBC URL parameter key for the request timeout.
     */
    public static final String KEY_REQUEST_TIMEOUT = "requesttimeout";
    /**
     * JDBC URL parameter key for the connection timeout.
     */
    public static final String KEY_CONNECT_TIMEOUT = "connecttimeout";
    /**
     * JDBC URL parameter key for the Nagle's algorithm enabling.
     */
    public static final String KEY_TCP_NO_DELAY = "tcpnodelay";
    /**
     * JDBC URL parameter key for the TCP keep-alive enabling.
     */
    public static final String KEY_KEEP_ALIVE = "keepalive";
    /**
     * JDBC URL parameter key for the configuration file.
     */
    public static final String KEY_CONFIG_FILE = "configfile";
    /**
     * JDBC URL parameter key for the compliance mode.
     */
    public static final String KEY_COMPLIANCE_MODE = "compliancemode";

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
    public static final String TAG_CONNECTION_RETRIES = "retries";
    public static final String TAG_ENABLE_SSL = "enableSsl";
    public static final String TAG_SSL_ENGINE_FACTORY = "sslEngineFactory";
    public static final String TAG_SSL_HOSTNAME_VERIFICATION = "hostnameVerification";
    public static final String TAG_CLOUD_SECURE_CONNECT_BUNDLE = "secureConnectBundle";
    public static final String TAG_CONFIG_FILE = "configFile";
    public static final String TAG_REQUEST_TIMEOUT = "requestTimeout";
    public static final String TAG_CONNECT_TIMEOUT = "connectTimeout";
    public static final String TAG_TCP_NO_DELAY = "tcpNoDelay";
    public static final String TAG_KEEP_ALIVE = "keepAlive";
    public static final String TAG_COMPLIANCE_MODE = "complianceMode";

    public static final String JSSE_TRUSTSTORE_PROPERTY = "javax.net.ssl.trustStore";
    public static final String JSSE_TRUSTSTORE_PASSWORD_PROPERTY = "javax.net.ssl.trustStorePassword";
    public static final String JSSE_KEYSTORE_PROPERTY = "javax.net.ssl.keyStore";
    public static final String JSSE_KEYSTORE_PASSWORD_PROPERTY = "javax.net.ssl.keyStorePassword";

    /**
     * {@code NULL} CQL keyword.
     */
    public static final String NULL_KEYWORD = "NULL";

    static final String WAS_CLOSED_CONN = "Method was called on a closed Connection.";
    static final String WAS_CLOSED_STMT = "Method was called on a closed Statement.";
    static final String WAS_CLOSED_RS = "Method was called on a closed ResultSet.";
    static final String NO_INTERFACE = "No object was found that matched the provided interface: %s";
    static final String NO_TRANSACTIONS = "The Cassandra implementation does not support transactions.";
    static final String ALWAYS_AUTOCOMMIT = "The Cassandra implementation is always in auto-commit mode.";
    static final String BAD_TIMEOUT = "The timeout value was less than zero.";
    static final String NOT_SUPPORTED = "The Cassandra implementation does not support this method.";
    static final String NO_GEN_KEYS =
        "The Cassandra implementation does not currently support returning generated keys.";
    static final String NO_MULTIPLE =
        "The Cassandra implementation does not currently support multiple open Result Sets.";
    static final String NO_RESULT_SET =
        "No ResultSet returned from the CQL statement passed in an 'executeQuery()' method.";
    static final String BAD_KEEP_RS =
        "The argument for keeping the current result set: %s is not a valid value.";
    static final String BAD_TYPE_RS = "The argument for result set type: %s is not a valid value.";
    static final String BAD_CONCURRENCY_RS =
        "The argument for result set concurrency: %s is not a valid value.";
    static final String BAD_HOLD_RS =
        "The argument for result set holdability: %s is not a valid value.";
    static final String BAD_FETCH_DIR = "Fetch direction value of: %s is illegal.";
    static final String BAD_AUTO_GEN = "Auto key generation value of: %s is illegal.";
    static final String BAD_FETCH_SIZE = "Fetch size of: %s rows may not be negative.";
    static final String MUST_BE_POSITIVE =
        "Index must be a positive number less or equal the count of returned columns: %d";
    static final String VALID_LABELS = "Name provided was not in the list of valid column labels: %s";
    static final String HOST_IN_URL =
        "Connection url must specify a host, e.g. jdbc:cassandra://localhost:9042/keyspace";
    static final String HOST_REQUIRED = "A 'host' name is required to build a Connection.";
    static final String BAD_KEYSPACE =
        "Keyspace names must be composed of alphanumerics and underscores (parsed: '%s').";
    static final String URI_IS_SIMPLE =
        "Connection url may only include host, port, and keyspace, consistency and version option, e.g. "
            + "jdbc:cassandra://localhost:9042/keyspace?version=3.0.0&consistency=ONE";
    static final String SECURECONENCTBUNDLE_REQUIRED = "A 'secureconnectbundle' parameter is required.";
    static final String FORWARD_ONLY = "Can not position cursor with a type of TYPE_FORWARD_ONLY.";
    static final String MALFORMED_URL = "The string '%s' is not a valid URL.";
    static final String SSL_CONFIG_FAILED = "Unable to configure SSL: %s.";

    static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    static ObjectMapper objectMapperInstance = null;

    private Utils() {
        // Private constructor to hide the public one.
    }

    /**
     * Gets a property value from the Cassandra JDBC driver properties file.
     *
     * @param name The name of the property.
     * @return The property value or an empty string the value cannot be retrieved.
     */
    public static String getDriverProperty(final String name) {
        try (final InputStream propertiesFile =
                 Utils.class.getClassLoader().getResourceAsStream(JDBC_DRIVER_PROPERTIES_FILE)) {
            final Properties driverProperties = new Properties();
            driverProperties.load(propertiesFile);
            return driverProperties.getProperty(name, StringUtils.EMPTY);
        } catch (IOException ex) {
            LOG.error("Unable to get JDBC driver property: {}.", name, ex);
            return StringUtils.EMPTY;
        }
    }

    /**
     * Gets a part of a version string.
     * <p>
     *     It uses the dot character as separator to parse the different parts of a version (major, minor, patch).
     * </p>
     *
     * @param version The version string (for example X.Y.Z).
     * @param part The part of the version to extract (for the semantic versioning, use 0 for the major version, 1 for
     *             the minor and 2 for the patch).
     * @return The requested part of the version, or 0 if the requested part cannot be parsed correctly.
     */
    public static int parseVersion(final String version, final int part) {
        if (StringUtils.isBlank(version) || StringUtils.countMatches(version, ".") < part || part < 0) {
            return 0;
        } else {
            try {
                return Integer.parseInt(version.split("\\.")[part]);
            } catch (final NumberFormatException ex) {
                LOG.error("Unable to parse version: {}", version);
                return 0;
            }
        }
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
     * @return A "sub-name" portion of a JDBC URL (for example: //myhost:9160/Test1?version=3.0.0).
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
            LOG.trace("Sub-name: '{}' created from: {}", uri.toString(), props);
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
    static String makeQueryString(final Properties props) {
        final StringBuilder sb = new StringBuilder();
        final String version = props.getProperty(TAG_CQL_VERSION);
        final String consistency = props.getProperty(TAG_CONSISTENCY_LEVEL);
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

    /**
     * Gets a pre-configured {@link ObjectMapper} for JSON support.
     *
     * @return A pre-configured {@link ObjectMapper} for JSON support.
     */
    public static ObjectMapper getObjectMapper() {
        if (objectMapperInstance != null) {
            return objectMapperInstance;
        } else {
            final ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            objectMapper.registerModule(new JavaTimeModule());
            final SimpleModule cassandraExtensionsModule = new SimpleModule();
            cassandraExtensionsModule.addDeserializer(ByteBuffer.class, new CassandraBlobDeserializer());
            cassandraExtensionsModule.addDeserializer(LocalDate.class, new CassandraDateDeserializer());
            cassandraExtensionsModule.addDeserializer(LocalTime.class, new CassandraTimeDeserializer());
            cassandraExtensionsModule.addDeserializer(OffsetDateTime.class, new CassandraDateTimeDeserializer());
            cassandraExtensionsModule.addSerializer(ByteBuffer.class, new CassandraBlobSerializer());
            objectMapper.registerModule(cassandraExtensionsModule);
            objectMapperInstance = objectMapper;
            return objectMapper;
        }
    }

}
