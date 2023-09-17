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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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

}
