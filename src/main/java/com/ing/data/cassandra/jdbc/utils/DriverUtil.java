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

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.ing.data.cassandra.jdbc.CassandraConnection;
import com.ing.data.cassandra.jdbc.codec.BigintToBigDecimalCodec;
import com.ing.data.cassandra.jdbc.codec.DecimalToDoubleCodec;
import com.ing.data.cassandra.jdbc.codec.FloatToDoubleCodec;
import com.ing.data.cassandra.jdbc.codec.IntToLongCodec;
import com.ing.data.cassandra.jdbc.codec.LongToIntCodec;
import com.ing.data.cassandra.jdbc.codec.SmallintToIntCodec;
import com.ing.data.cassandra.jdbc.codec.SqlDateCodec;
import com.ing.data.cassandra.jdbc.codec.SqlTimeCodec;
import com.ing.data.cassandra.jdbc.codec.SqlTimestampCodec;
import com.ing.data.cassandra.jdbc.codec.TimestampToLongCodec;
import com.ing.data.cassandra.jdbc.codec.TinyintToIntCodec;
import com.ing.data.cassandra.jdbc.codec.TinyintToShortCodec;
import com.ing.data.cassandra.jdbc.codec.VarintToIntCodec;
import com.ing.data.cassandra.jdbc.metadata.VersionedMetadata;
import org.apache.commons.lang3.StringUtils;
import org.semver4j.Semver;
import org.semver4j.range.RangeExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.DriverPropertyInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_PASSWORD;
import static com.ing.data.cassandra.jdbc.utils.WarningConstants.DRIVER_PROPERTY_NOT_FOUND;
import static com.ing.data.cassandra.jdbc.utils.WarningConstants.URL_REDACTION_FAILED;
import static org.apache.commons.collections4.ListUtils.emptyIfNull;
import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * A set of static utility methods and constants used by the JDBC driver.
 */
public final class DriverUtil {

    /**
     * Properties file name containing some properties relative to this JDBC wrapper (such as JDBC driver version,
     * name, etc.).
     */
    public static final String JDBC_DRIVER_PROPERTIES_FILE = "jdbc-driver.properties";

    /**
     * The JSSE property used to retrieve the trust store when SSL is enabled on the database connection using
     * JSSE properties.
     */
    public static final String JSSE_TRUSTSTORE_PROPERTY = "javax.net.ssl.trustStore";

    /**
     * The JSSE property used to retrieve the trust store password when SSL is enabled on the database connection using
     * JSSE properties.
     */
    public static final String JSSE_TRUSTSTORE_PASSWORD_PROPERTY = "javax.net.ssl.trustStorePassword";

    /**
     * The JSSE property used to retrieve the key store when SSL is enabled on the database connection using
     * JSSE properties.
     */
    public static final String JSSE_KEYSTORE_PROPERTY = "javax.net.ssl.keyStore";

    /**
     * The JSSE property used to retrieve the key store password when SSL is enabled on the database connection using
     * JSSE properties.
     */
    public static final String JSSE_KEYSTORE_PASSWORD_PROPERTY = "javax.net.ssl.keyStorePassword";

    /**
     * {@code NULL} CQL keyword.
     */
    public static final String NULL_KEYWORD = "NULL";

    /**
     * Cassandra version 5.0. Used for types and built-in functions versioning.
     */
    public static final String CASSANDRA_5 = "5.0";

    /**
     * Cassandra version 4.0. Used for types and built-in functions versioning.
     */
    public static final String CASSANDRA_4 = "4.0";

    /**
     * Comma character.
     */
    public static final String COMMA = ",";

    /**
     * Single quote character.
     */
    public static final String SINGLE_QUOTE = "'";

    /**
     * Default username for connection to AstraDB instance with token.
     */
    public static final String DEFAULT_ASTRA_DB_USER = "token";

    /**
     * UUIDv1 pattern, used by {@code TIMEUUID} CQL type.
     */
    public static final Pattern UUID_V1_PATTERN = Pattern.compile(
        "^[0-9A-F]{8}-[0-9A-F]{4}-1[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$", Pattern.CASE_INSENSITIVE);

    /**
     * UUIDv4 pattern, used by {@code UUID} CQL type.
     */
    public static final Pattern UUID_V4_PATTERN = Pattern.compile(
        "^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$", Pattern.CASE_INSENSITIVE);

    /**
     * Duration format pattern, used by {@code DURATION} CQL type.
     * See: <a href="https://cassandra.apache.org/doc/stable/cassandra/cql/types.html#durations">Duration CQL type</a>
     */
    public static final Pattern DURATION_FORMAT_PATTERN = Pattern.compile(
        "^(\\d+y)?(\\d+mo)?(\\d+w)?(\\d+d)?(\\d+h)?(\\d+m)?(\\d+s)?(\\d+ms)?(\\d+us)?(\\d+ns)?$"
    );

    /**
     * ISO-8601 duration format pattern, used by {@code DURATION} CQL type.
     * See: <a href="https://cassandra.apache.org/doc/stable/cassandra/cql/types.html#durations">Duration CQL type</a>
     */
    public static final Pattern DURATION_ISO8601_FORMAT_PATTERN = Pattern.compile(
        "^P((\\d+Y)?(\\d+M)?(\\d+D)?T(\\d+H)?(\\d+M)?(\\d+S)?)?|(\\d+W)?$"
    );

    /**
     * Alternative ISO-8601 duration format pattern, used by {@code DURATION} CQL type.
     * See: <a href="https://cassandra.apache.org/doc/stable/cassandra/cql/types.html#durations">Duration CQL type</a>
     */
    public static final Pattern DURATION_ISO8601_ALT_FORMAT_PATTERN = Pattern.compile(
        "^P\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}$");

    static final Logger LOG = LoggerFactory.getLogger(DriverUtil.class);

    private DriverUtil() {
        // Private constructor to hide the public one.
    }

    /**
     * Lists the pre-configured codecs used by this driver.
     *
     * @return All the pre-configured codecs.
     */
    public static List<TypeCodec<?>> listPreconfiguredCodecs() {
        final List<TypeCodec<?>> codecs = new ArrayList<>();
        codecs.add(new TimestampToLongCodec());
        codecs.add(new LongToIntCodec());
        codecs.add(new IntToLongCodec());
        codecs.add(new BigintToBigDecimalCodec());
        codecs.add(new DecimalToDoubleCodec());
        codecs.add(new FloatToDoubleCodec());
        codecs.add(new VarintToIntCodec());
        codecs.add(new SmallintToIntCodec());
        codecs.add(new TinyintToIntCodec());
        codecs.add(new TinyintToShortCodec());
        codecs.add(new SqlTimestampCodec());
        codecs.add(new SqlDateCodec());
        codecs.add(new SqlTimeCodec());
        return codecs;
    }

    /**
     * Gets a property value from the Cassandra JDBC driver properties file.
     *
     * @param name The name of the property.
     * @return The property value or an empty string the value cannot be retrieved.
     */
    public static String getDriverProperty(final String name) {
        try (final InputStream propertiesFile =
                 DriverUtil.class.getClassLoader().getResourceAsStream(JDBC_DRIVER_PROPERTIES_FILE)) {
            final Properties driverProperties = new Properties();
            driverProperties.load(propertiesFile);
            return driverProperties.getProperty(name, EMPTY);
        } catch (final IOException ex) {
            LOG.warn(DRIVER_PROPERTY_NOT_FOUND, name, ex);
            return EMPTY;
        }
    }

    /**
     * Gets the {@link Semver} representation of a version string.
     * <p>
     *     It uses the dot character as separator to parse the different parts of a version (major, minor, patch).
     * </p>
     *
     * @param version The version string (for example X.Y.Z).
     * @return The parsed version, or {@link Semver#ZERO} if the string cannot be parsed correctly.
     */
    public static Semver safeParseVersion(final String version) {
        if (StringUtils.isBlank(version)) {
            return Semver.ZERO;
        } else {
            final Semver parsedVersion = Semver.coerce(version);
            if (parsedVersion == null) {
                return Semver.ZERO;
            }
            return parsedVersion;
        }
    }

    /**
     * Checks whether the database metadata (CQL type or built-in function) exists in the current database version.
     *
     * @param dbVersion         The version of the Cassandra database the driver is currently connected to.
     * @param versionedMetadata The database metadata to check.
     * @return {@code true} if the database metadata exists in the current database version, {@code false} otherwise.
     */
    public static boolean existsInDatabaseVersion(final String dbVersion,
                                                  final VersionedMetadata versionedMetadata) {
        final Semver parseDatabaseVersion = Semver.coerce(dbVersion);
        if (parseDatabaseVersion == null) {
            return false;
        }
        Semver minVersion = Semver.ZERO;
        if (versionedMetadata.isValidFrom() != null) {
            minVersion = versionedMetadata.isValidFrom();
        }
        final RangeExpression validRange = RangeExpression.greaterOrEqual(minVersion);
        if (versionedMetadata.isInvalidFrom() != null) {
            validRange.and(RangeExpression.less(versionedMetadata.isInvalidFrom()));
        }
        return parseDatabaseVersion.satisfies(validRange);
    }

    /**
     * Builds an alphabetically sorted and comma-separated list of metadata (such as built-in functions or CQL
     * keywords) existing in the specified Cassandra version.
     *
     * @param metadataList The list of possible metadata to format.
     * @param dbVersion    The version of the Cassandra database the driver is currently connected to.
     * @param connection   The database connection.
     * @return The formatted list of metadata.
     */
    public static String buildMetadataList(final List<VersionedMetadata> metadataList, final String dbVersion,
                                           final CassandraConnection connection) {
        return metadataList.stream()
            .filter(metadata -> existsInDatabaseVersion(dbVersion, metadata)
                && metadata.fulfillAdditionalCondition(connection))
            .map(VersionedMetadata::getName)
            .sorted()
            .collect(Collectors.joining(COMMA));
    }

    /**
     * Builds an alphabetically sorted and comma-separated list of metadata (such as built-in functions or CQL
     * keywords) existing in the specified Cassandra version.
     *
     * @param metadataList The list of possible metadata to format.
     * @param dbVersion    The version of the Cassandra database the driver is currently connected to.
     * @return The formatted list of metadata.
     */
    public static String buildMetadataList(final List<VersionedMetadata> metadataList, final String dbVersion) {
        return buildMetadataList(metadataList, dbVersion, null);
    }

    /**
     * Builds an instance of {@link DriverPropertyInfo} for the given driver property and value.
     *
     * @param propertyName The property name.
     * @param value        The current value of the property.
     * @return The driver property info.
     */
    public static DriverPropertyInfo buildPropertyInfo(final String propertyName, final Object value) {
        final DriverPropertyInfo propertyInfo = new DriverPropertyInfo(propertyName,
            Objects.toString(value, EMPTY));
        final String driverPropertyDefinition = "driver.properties." + propertyName;

        final String propertyChoices = getDriverProperty(driverPropertyDefinition + ".choices");
        if (StringUtils.isNotBlank(propertyChoices)) {
            propertyInfo.choices = propertyChoices.split(COMMA);
        }
        propertyInfo.required = Boolean.getBoolean(getDriverProperty(driverPropertyDefinition + ".required"));
        propertyInfo.description = getDriverProperty(driverPropertyDefinition);
        return propertyInfo;
    }

    /**
     * Returns a string representation of the provided driver properties with redacted values for sensitive properties
     * such as passwords.
     *
     * @param properties The driver properties.
     * @return The string representation of the properties, without sensitive values.
     */
    public static String toStringWithoutSensitiveValues(final Properties properties) {
        final Properties withRedactedSensitiveValues = (Properties) properties.clone();
        if (withRedactedSensitiveValues.containsKey(TAG_PASSWORD)) {
            withRedactedSensitiveValues.setProperty(TAG_PASSWORD, "***");
        }
        return withRedactedSensitiveValues.toString();
    }

    /**
     * Returns a redacted version of the provided JDBC URL for sensitive query parameters such as passwords.
     *
     * @param jdbcUrl The JDBC URL.
     * @return The redacted JDBC URL.
     */
    public static String redactSensitiveValuesInJdbcUrl(final String jdbcUrl) {
        if (!jdbcUrl.contains("://")) {
            LOG.warn(URL_REDACTION_FAILED);
            return "<invalid URL>";
        }
        try {
            // Extract scheme as composed schemes like 'jdbc:cassandra' are not supported, then only keep the second
            // part starting with double slashes as the URI to parse.
            final String[] splitUrl = jdbcUrl.split("://");
            final URI uri = new URI("//" + splitUrl[1]);

            // Redact sensitive values, then re-build the entire JDBC URL.
            final String redactedQuery = Arrays.stream(StringUtils.defaultIfBlank(uri.getQuery(), EMPTY).split("&"))
                .map(param -> {
                    if (param.startsWith(TAG_PASSWORD + "=")) {
                        return TAG_PASSWORD + "=***";
                    }
                    return param;
                })
                .collect(Collectors.joining("&"));
            return new URI(
                splitUrl[0], uri.getAuthority(), uri.getPath(), redactedQuery, uri.getFragment()
            ).toString();
        } catch (final URISyntaxException e) {
            LOG.warn(URL_REDACTION_FAILED);
            return "<invalid URL>";
        }
    }

    /**
     * Registers the given codecs to the specified CQL session, checking they are not already registered before
     * registering them.
     *
     * @param cqlSession The CQL session.
     * @param codecs     The codecs to register.
     */
    public static void safelyRegisterCodecs(final Session cqlSession, final List<TypeCodec<?>> codecs) {
        final DefaultCodecRegistry codecRegistry = (DefaultCodecRegistry) cqlSession.getContext().getCodecRegistry();
        codecs.stream()
            .filter(codec -> {
                try {
                    codecRegistry.codecFor(codec.getCqlType(), codec.getJavaType());
                    // The codec has been found, exclude it form the list of codecs to register.
                    return false;
                } catch (final CodecNotFoundException e) {
                    // If the codec is not registered yet, include it into the filtered codecs to register it.
                    return true;
                }
            })
            .forEach(codecRegistry::register);
    }

    /**
     * Formats a list of {@link ContactPoint} to a comma-separated list of strings representing the contact points.
     *
     * @param contactPoints The contact points.
     * @return The formatted list of contact points.
     */
    public static String formatContactPoints(final List<ContactPoint> contactPoints) {
        return emptyIfNull(contactPoints).stream()
            .map(ContactPoint::toString)
            .collect(Collectors.joining(", "));
    }

}
