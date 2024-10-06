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
import com.ing.data.cassandra.jdbc.metadata.VersionedMetadata;
import org.apache.commons.lang3.StringUtils;
import org.semver4j.RangesExpression;
import org.semver4j.Semver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverPropertyInfo;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_PASSWORD;

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

    static final Logger LOG = LoggerFactory.getLogger(DriverUtil.class);

    private DriverUtil() {
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
                 DriverUtil.class.getClassLoader().getResourceAsStream(JDBC_DRIVER_PROPERTIES_FILE)) {
            final Properties driverProperties = new Properties();
            driverProperties.load(propertiesFile);
            return driverProperties.getProperty(name, StringUtils.EMPTY);
        } catch (IOException ex) {
            LOG.error("Unable to get JDBC driver property: {}.", name, ex);
            return StringUtils.EMPTY;
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
        final RangesExpression validRange = RangesExpression.greaterOrEqual(minVersion);
        if (versionedMetadata.isInvalidFrom() != null) {
            validRange.and(RangesExpression.less(versionedMetadata.isInvalidFrom()));
        }
        return parseDatabaseVersion.satisfies(validRange);
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
        return metadataList.stream()
            .filter(metadata -> existsInDatabaseVersion(dbVersion, metadata))
            .map(VersionedMetadata::getName)
            .sorted()
            .collect(Collectors.joining(","));
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
            Objects.toString(value, StringUtils.EMPTY));
        final String driverPropertyDefinition = "driver.properties." + propertyName;

        final String propertyChoices = getDriverProperty(driverPropertyDefinition + ".choices");
        if (StringUtils.isNotBlank(propertyChoices)) {
            propertyInfo.choices = propertyChoices.split(",");
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

}
