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

import java.io.InputStream;
import java.util.Properties;

/**
 * Load the connection details from the properties file.
 */
public final class ConnectionDetails {

    private static final ConnectionDetails INSTANCE = new ConnectionDetails();

    private final String host;
    private final int port;

    private ConnectionDetails() {
        final Properties properties = new Properties();
        try (final InputStream stream = getClass().getResourceAsStream(getClass().getSimpleName() + ".properties")) {
            properties.load(stream);
        } catch (final Exception e) {
            // ignore, we'll use the defaults
        }

        host = properties.getProperty("host", "localhost");
        int port;
        try {
            port = Integer.parseInt(properties.getProperty("port", "9042"));
        } catch (final NumberFormatException e) {
            port = 9042;
        }
        this.port = port;
    }

    public static String getHost() {
        return INSTANCE.host;
    }

    public static int getPort() {
        return INSTANCE.port;
    }
}
