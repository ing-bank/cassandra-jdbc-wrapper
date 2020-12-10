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
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A set of static utility methods and constants used by the JDBC Suite, and various default values and error message
 * strings that can be shared across classes.
 */
public final class Utils {
    private static final Pattern KEYSPACE_PATTERN = Pattern.compile("USE (\\w+);?",
        Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern SELECT_PATTERN = Pattern.compile("(?:SELECT|DELETE)\\s+.+\\s+FROM\\s+(\\w+).*",
        Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern UPDATE_PATTERN = Pattern.compile("UPDATE\\s+(\\w+)\\s+.*", Pattern.CASE_INSENSITIVE);

    public static final String PROTOCOL = "jdbc:cassandra:";
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9042;
    public static final ConsistencyLevel DEFAULT_CONSISTENCY = DefaultConsistencyLevel.ONE;

    public static final String KEY_VERSION = "version";
    public static final String KEY_CONSISTENCY = "consistency";
    public static final String KEY_PRIMARY_DC = "primarydc";
    public static final String KEY_BACKUP_DC = "backupdc";
    public static final String KEY_CONNECTION_RETRIES = "retries";
    public static final String KEY_LOADBALANCING_POLICY = "loadbalancing";
    public static final String KEY_LOCAL_DATACENTER = "localdatacenter";
    public static final String KEY_RETRY_POLICY = "retry";
    public static final String KEY_RECONNECT_POLICY = "reconnection";
    public static final String KEY_DEBUG = "debug";

    public static final String TAG_DESCRIPTION = "description";
    public static final String TAG_USER = "user";
    public static final String TAG_PASSWORD = "password";
    public static final String TAG_DATABASE_NAME = "databaseName";
    public static final String TAG_SERVER_NAME = "serverName";
    public static final String TAG_PORT_NUMBER = "portNumber";
    public static final String TAG_ACTIVE_CQL_VERSION = "activeCqlVersion";
    public static final String TAG_CQL_VERSION = "cqlVersion";
    public static final String TAG_BUILD_VERSION = "buildVersion";
    public static final String TAG_THRIFT_VERSION = "thriftVersion";
    public static final String TAG_CONSISTENCY_LEVEL = "consistencyLevel";
    public static final String TAG_LOADBALANCING_POLICY = "loadBalancing";
    public static final String TAG_LOCAL_DATACENTER = "localDatacenter";
    public static final String TAG_RETRY_POLICY = "retry";
    public static final String TAG_RECONNECT_POLICY = "reconnection";
    public static final String TAG_DEBUG = "debug";

    public static final String TAG_PRIMARY_DC = "primaryDatacenter";
    public static final String TAG_BACKUP_DC = "backupDatacenter";
    public static final String TAG_CONNECTION_RETRIES = "retries";

    protected static final String WAS_CLOSED_CON = "Method was called on a closed Connection.";
    protected static final String WAS_CLOSED_STMT = "Method was called on a closed Statement.";
    protected static final String WAS_CLOSED_RSLT = "Method was called on a closed ResultSet.";
    protected static final String NO_INTERFACE = "No object was found that matched the provided interface: %s";
    protected static final String NO_TRANSACTIONS = "The Cassandra implementation does not support transactions.";
    protected static final String NO_SERVER = "No Cassandra server is available.";
    protected static final String ALWAYS_AUTOCOMMIT = "The Cassandra implementation is always in auto-commit mode.";
    protected static final String BAD_TIMEOUT = "The timeout value was less than zero.";
    protected static final String SCHEMA_MISMATCH = "Schema does not match across nodes, (try again later).";
    public static final String NOT_SUPPORTED = "The Cassandra implementation does not support this method.";
    protected static final String NO_GEN_KEYS =
        "The Cassandra implementation does not currently support returning generated  keys.";
    protected static final String NO_BATCH =
        "The Cassandra implementation does not currently support this batch in Statement.";
    protected static final String NO_MULTIPLE =
        "The Cassandra implementation does not currently support multiple open Result Sets.";
    protected static final String NO_VALIDATOR = "Could not find key validator for: %s.%s";
    protected static final String NO_COMPARATOR = "Could not find key comparator for: %s.%s";
    protected static final String NO_RESULTSET =
        "No ResultSet returned from the CQL statement passed in an 'executeQuery()' method.";
    protected static final String NO_UPDATE_COUNT =
        "No Update Count was returned from the CQL statement passed in an 'executeUpdate()' method.";
    protected static final String NO_CF =
        "No column family reference could be extracted from the provided CQL statement.";
    protected static final String BAD_KEEP_RSET =
        "The argument for keeping the current result set : %s is not a valid value.";
    protected static final String BAD_TYPE_RSET = "The argument for result set type : %s is not a valid value.";
    protected static final String BAD_CONCUR_RSET =
        "The argument for result set concurrency : %s is not a valid value.";
    protected static final String BAD_HOLD_RSET =
        "The argument for result set holdability : %s is not a valid value.";
    protected static final String BAD_FETCH_DIR = "Fetch direction value of : %s is illegal.";
    protected static final String BAD_AUTO_GEN = "Auto key generation value of : %s is illegal.";
    protected static final String BAD_FETCH_SIZE = "Fetch size of : %s rows may not be negative.";
    protected static final String MUST_BE_POSITIVE =
        "Index must be a positive number less or equal the count of returned columns: %d";
    protected static final String VALID_LABELS = "Name provided was not in the list of valid column labels: %s";
    protected static final String NOT_TRANSLATABLE = "Column was stored in %s format which is not translatable to %s";
    protected static final String NOT_BOOLEAN = "String value was neither 'true' nor 'false' :  %s";
    protected static final String HOST_IN_URL =
        "Connection url must specify a host, e.g., jdbc:cassandra://localhost:9042/Keyspace";
    protected static final String HOST_REQUIRED = "A 'host' name is required to build a Connection.";
    protected static final String BAD_KEYSPACE =
        "Keyspace names must be composed of alphanumerics and underscores (parsed: '%s').";
    protected static final String URI_IS_SIMPLE =
        "Connection url may only include host, port, and keyspace, consistency and version option, e.g., "
            + "jdbc:cassandra://localhost:9042/Keyspace?version=3.0.0&consistency=ONE";
    protected static final String NOT_OPTION = "Connection url only supports the 'version' and 'consistency' options.";
    protected static final String FORWARD_ONLY = "Can not position cursor with a type of TYPE_FORWARD_ONLY.";

    protected static final Logger log = LoggerFactory.getLogger(Utils.class);

    /**
     * Parse a URL for the Cassandra JDBC Driver
     * <p></p>
     * The URL must start with the Protocol: "jdbc:cassandra:"
     * The URI part(the "Subname") must contain a host and an optional port and optional keyspace name
     * ie. "//localhost:9160/Test1"
     *
     * @param url The full JDBC URL to be parsed
     * @return A list of properties that were parsed from the Subname
     * @throws SQLException when something went wrong during the URL parsing.
     */
    public static Properties parseURL(final String url) throws SQLException {
        final Properties props = new Properties();

        if (!(url == null)) {
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

            final int port = uri.getPort() >= 0 ? uri.getPort() : DEFAULT_PORT;
            props.setProperty(TAG_PORT_NUMBER, String.valueOf(port));

            String keyspace = uri.getPath();
            if ((keyspace != null) && (!keyspace.isEmpty())) {
                if (keyspace.startsWith("/")) keyspace = keyspace.substring(1);
                if (!keyspace.matches("[a-zA-Z]\\w+"))
                    throw new SQLNonTransientConnectionException(String.format(BAD_KEYSPACE, keyspace));
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
                if (params.containsKey(KEY_LOADBALANCING_POLICY)) {
                    props.setProperty(TAG_LOADBALANCING_POLICY, params.get(KEY_LOADBALANCING_POLICY));
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
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("URL : '{}' parses to: {}", url, props);
        }

        return props;
    }

    /**
     * Create a "Subname" portion of a JDBC URL from properties.
     *
     * @param props A Properties file containing all the properties to be considered.
     * @return A constructed "Subname" portion of a JDBC URL in the form of a CLI
     * (ie: //myhost:9160/Test1?version=3.0.0)
     * @throws SQLException when something went wrong during the sub-name creation.
     */
    public static String createSubName(final Properties props) throws SQLException {
        // make keyspace always start with a "/" for URI
        String keyspace = props.getProperty(TAG_DATABASE_NAME);

        // if keyspace is null then do not bother ...
        if (keyspace != null)
            if (!keyspace.startsWith("/")) keyspace = "/" + keyspace;

        final String host = props.getProperty(TAG_SERVER_NAME);
        if (host == null) {
            throw new SQLNonTransientConnectionException(HOST_REQUIRED);
        }

        // construct a valid URI from parts...
        final URI uri;
        try {
            uri = new URI(
                null,
                null,
                host,
                props.getProperty(TAG_PORT_NUMBER) == null ? DEFAULT_PORT
                    : Integer.parseInt(props.getProperty(TAG_PORT_NUMBER)),
                keyspace,
                makeQueryString(props),
                null);
        } catch (final Exception e) {
            throw new SQLNonTransientConnectionException(e);
        }

        if (log.isTraceEnabled()) {
            log.trace("Subname : '{}' created from : {}", uri.toString(), props);
        }

        return uri.toString();
    }

    /**
     * Determine the current keyspace by inspecting the CQL string to see if a USE statement is provided; which would
     * change the keyspace.
     *
     * @param cql     A CQL query string
     * @param current The current keyspace stored as state in the connection
     * @return the provided keyspace name or the keyspace from the contents of the CQL string
     */
    public static String determineCurrentKeyspace(final String cql, final String current) {
        String ks = current;
        final Matcher isKeyspace = KEYSPACE_PATTERN.matcher(cql);
        if (isKeyspace.matches()) ks = isKeyspace.group(1);
        return ks;
    }

    /**
     * Determine the current column family by inspecting the CQL to find a CF reference.
     *
     * @param cql A CQL query string
     * @return The column family name from the contents of the CQL string or null in none was found
     */
    public static String determineCurrentColumnFamily(final String cql) {
        String cf = null;
        final Matcher isSelect = SELECT_PATTERN.matcher(cql);
        if (isSelect.matches()) cf = isSelect.group(1);
        final Matcher isUpdate = UPDATE_PATTERN.matcher(cql);
        if (isUpdate.matches()) cf = isUpdate.group(1);
        return cf;
    }

    /**
     * Utility method to pack bytes into a byte buffer from a list of ByteBuffers
     *
     * @param buffers  A list of ByteBuffers representing the elements to pack
     * @param elements The count of the elements
     * @param size     The size in bytes of the result buffer
     * @return The packed ByteBuffer
     */
    protected static ByteBuffer pack(final List<ByteBuffer> buffers, final int elements, final int size) {
        final ByteBuffer result = ByteBuffer.allocate(2 + size);
        result.putShort((short) elements);
        for (final ByteBuffer bb : buffers) {
            result.putShort((short) bb.remaining());
            result.put(bb.duplicate());
        }
        return (ByteBuffer) result.flip();
    }

    protected static String makeQueryString(final Properties props) {
        final StringBuilder sb = new StringBuilder();
        final String version = (props.getProperty(TAG_CQL_VERSION));
        final String consistency = (props.getProperty(TAG_CONSISTENCY_LEVEL));
        if (consistency != null) {
            sb.append(KEY_CONSISTENCY).append("=").append(consistency);
        }
        if (version != null) {
            if (sb.length() != 0) sb.append("&");
            sb.append(KEY_VERSION).append("=").append(version);
        }

        return (sb.length() == 0) ? null : sb.toString().trim();
    }

    protected static Map<String, String> parseQueryPart(final String query) throws SQLException {
        final Map<String, String> params = new HashMap<>();
        for (final String param : query.split("&")) {
            try {
                final String[] pair = param.split("=");
                final String key = URLDecoder.decode(pair[0], "UTF-8").toLowerCase();
                String value = StringUtils.EMPTY;
                if (pair.length > 1) value = URLDecoder.decode(pair[1], "UTF-8");
                params.put(key, value);
            } catch (final UnsupportedEncodingException e) {
                throw new SQLSyntaxErrorException(e);
            }
        }
        return params;
    }

    public static LinkedHashSet<?> parseSet(final String itemType, final String value) {
        if ("varchar".equals(itemType) || "text".equals(itemType) || "ascii".equals(itemType)) {
            final LinkedHashSet<String> zeSet = new LinkedHashSet<>();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");
            for (final String val : values) {
                zeSet.add(val);
            }
            return zeSet;

        } else if ("bigint".equals(itemType)) {
            final LinkedHashSet<Long> zeSet = Sets.newLinkedHashSet();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeSet.add(Long.parseLong(val.trim()));
            }
            return zeSet;
        } else if ("varint".equals(itemType)) {
            final LinkedHashSet<BigInteger> zeSet = Sets.newLinkedHashSet();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeSet.add(BigInteger.valueOf(Long.parseLong(val.trim())));
            }
            return zeSet;
        } else if ("decimal".equals(itemType)) {
            final LinkedHashSet<BigDecimal> zeSet = Sets.newLinkedHashSet();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeSet.add(BigDecimal.valueOf(Double.parseDouble(val.trim())));
            }
            return zeSet;
        } else if ("double".equals(itemType)) {
            final LinkedHashSet<Double> zeSet = Sets.newLinkedHashSet();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeSet.add(Double.parseDouble(val.trim()));
            }
            return zeSet;
        } else if ("float".equals(itemType)) {
            final LinkedHashSet<Float> zeSet = Sets.newLinkedHashSet();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeSet.add(Float.parseFloat(val.trim()));
            }
            return zeSet;
        } else if ("boolean".equals(itemType)) {
            final LinkedHashSet<Boolean> zeSet = Sets.newLinkedHashSet();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeSet.add(Boolean.parseBoolean(val.trim()));
            }
            return zeSet;
        } else if ("int".equals(itemType)) {
            final LinkedHashSet<Integer> zeSet = Sets.newLinkedHashSet();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeSet.add(Integer.parseInt(val.trim()));
            }
            return zeSet;
        } else if ("uuid".equals(itemType)) {
            final LinkedHashSet<UUID> zeSet = Sets.newLinkedHashSet();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeSet.add(UUID.fromString(val.trim()));
            }
            return zeSet;
        } else if ("timeuuid".equals(itemType)) {
            final LinkedHashSet<UUID> zeSet = Sets.newLinkedHashSet();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeSet.add(UUID.fromString(val.trim()));
            }
            return zeSet;
        }
        return null;
    }

    public static ArrayList<?> parseList(final String itemType, final String value) {
        if ("varchar".equals(itemType) || "text".equals(itemType) || "ascii".equals(itemType)) {
            final ArrayList<String> zeList = Lists.newArrayList();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");
            int i = 0;
            for (final String val : values) {
                if (i > 0 && val.startsWith(" ")) {
                    zeList.add(val.substring(1));
                } else {
                    zeList.add(val);
                }
                i++;
            }
            return zeList;
        } else if ("bigint".equals(itemType)) {
            final ArrayList<Long> zeList = Lists.newArrayList();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeList.add(Long.parseLong(val.trim()));
            }
            return zeList;
        } else if ("varint".equals(itemType)) {
            final ArrayList<BigInteger> zeList = Lists.newArrayList();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeList.add(BigInteger.valueOf(Long.parseLong(val.trim())));
            }
            return zeList;
        } else if ("decimal".equals(itemType)) {
            final ArrayList<BigDecimal> zeList = Lists.newArrayList();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeList.add(BigDecimal.valueOf(Double.parseDouble(val.trim())));
            }
            return zeList;
        } else if ("double".equals(itemType)) {
            final ArrayList<Double> zeList = Lists.newArrayList();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeList.add(Double.parseDouble(val.trim()));
            }
            return zeList;
        } else if ("float".equals(itemType)) {
            final ArrayList<Float> zeList = Lists.newArrayList();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeList.add(Float.parseFloat(val.trim()));
            }
            return zeList;
        } else if ("boolean".equals(itemType)) {
            final ArrayList<Boolean> zeList = Lists.newArrayList();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeList.add(Boolean.parseBoolean(val.trim()));
            }
            return zeList;
        } else if ("int".equals(itemType)) {
            final ArrayList<Integer> zeList = Lists.newArrayList();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeList.add(Integer.parseInt(val.trim()));
            }
            return zeList;
        } else if ("uuid".equals(itemType)) {
            final ArrayList<UUID> zeList = Lists.newArrayList();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeList.add(UUID.fromString(val.trim()));
            }
            return zeList;
        } else if ("timeuuid".equals(itemType)) {
            final ArrayList<UUID> zeList = Lists.newArrayList();
            final String[] values = value.replace("[", "").replace("]", "").split(", ");

            for (final String val : values) {
                zeList.add(UUID.fromString(val.trim()));
            }
            return zeList;
        }
        return null;
    }

    @SuppressWarnings({"boxing", "unchecked", "rawtypes"})
    public static HashMap<?, ?> parseMap(final String kType, final String vType, final String value) {
        //Parsing values looking like this :
        //{key1:val1, key2:val2}

        final Map zeMap = new HashMap();
        final String[] values = value.replace("{", "").replace("}", "").split(", ");
        final List keys = Lists.newArrayList();
        final List vals = Lists.newArrayList();

        for (final String val : values) {
            final String[] keyVal = val.split("=");
            if ("bigint".equals(kType)) {
                keys.add(Long.parseLong(keyVal[0]));
            } else if ("varint".equals(kType)) {
                keys.add(BigInteger.valueOf(Long.parseLong(keyVal[0])));
            } else if ("decimal".equals(kType)) {
                keys.add(BigDecimal.valueOf(Double.parseDouble(keyVal[0])));
            } else if ("double".equals(kType)) {
                keys.add(Double.parseDouble(keyVal[0]));
            } else if ("float".equals(kType)) {
                keys.add(Float.parseFloat(keyVal[0]));
            } else if ("boolean".equals(kType)) {
                keys.add(Boolean.parseBoolean(keyVal[0]));
            } else if ("int".equals(kType)) {
                keys.add(Integer.parseInt(keyVal[0]));
            } else if ("uuid".equals(kType)) {
                keys.add(UUID.fromString(keyVal[0]));
            } else if ("timeuuid".equals(kType)) {
                keys.add(UUID.fromString(keyVal[0]));
            } else {
                keys.add(keyVal[0]);
            }

            if ("bigint".equals(vType)) {
                vals.add(Long.parseLong(keyVal[1]));
            } else if ("varint".equals(vType)) {
                vals.add(BigInteger.valueOf(Long.parseLong(keyVal[1])));
            } else if ("decimal".equals(vType)) {
                vals.add(BigDecimal.valueOf(Double.parseDouble(keyVal[1])));
            } else if ("double".equals(vType)) {
                vals.add(Double.parseDouble(keyVal[1]));
            } else if ("float".equals(vType)) {
                vals.add(Float.parseFloat(keyVal[1]));
            } else if ("boolean".equals(vType)) {
                vals.add(Boolean.parseBoolean(keyVal[1]));
            } else if ("int".equals(vType)) {
                vals.add(Integer.parseInt(keyVal[1]));
            } else if ("uuid".equals(vType)) {
                vals.add(UUID.fromString(keyVal[1]));
            } else if ("timeuuid".equals(vType)) {
                vals.add(UUID.fromString(keyVal[1]));
            } else {
                vals.add(keyVal[1]);
            }

            zeMap.put(keys.get(keys.size() - 1), vals.get(vals.size() - 1));
        }

        return (HashMap<?, ?>) zeMap;
    }

    public static Map<DriverOption, Object> parseReconnectionPolicy(final String reconnectionPolicyString)
        throws SecurityException, IllegalArgumentException {
        final String policyRegex = "([a-zA-Z\\.]*Policy)(\\()(.*)(\\))";
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

    public static Map<DriverOption, Object> getReconnectionPolicy(String primaryReconnectionPolicy,
                                                                  final String parameters)
        throws SecurityException, IllegalArgumentException {
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
