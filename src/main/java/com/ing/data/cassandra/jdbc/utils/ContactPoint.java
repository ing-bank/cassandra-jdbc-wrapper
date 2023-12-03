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

import java.net.InetSocketAddress;
import java.util.Objects;

import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.DEFAULT_PORT;

/**
 * The representation of contact point in a Cassandra cluster.
 * <p>
 *     This class is used to parse JDBC URL and extract hosts and ports of the contact points.
 * </p>
 */
public final class ContactPoint {

    private final String host;
    private final int port;

    private ContactPoint(final String host, final int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Instantiates a contact point from the host and port.
     *
     * @param host The hostname.
     * @param port The port. If {@code null}, the default Cassandra port ({@value JdbcUrlUtil#DEFAULT_PORT}) is used.
     * @return The contact point representation.
     */
    public static ContactPoint of(final String host, final Integer port) {
        if (port == null) {
            return new ContactPoint(host, DEFAULT_PORT);
        }
        return new ContactPoint(host, port);
    }

    /**
     * Gets the hostname of the contact point.
     *
     * @return The hostname of the contact point.
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Gets the port of the contact point.
     *
     * @return The port of the contact point.
     */
    public Integer getPort() {
        return this.port;
    }

    /**
     * Converts the contact point into a socket address usable to instantiate a connection to a Cassandra cluster.
     *
     * @return The socket address corresponding to the contact point.
     */
    public InetSocketAddress toInetSocketAddress() {
        return InetSocketAddress.createUnresolved(this.host, this.port);
    }

    @Override
    public String toString() {
        return String.format("%s:%d", this.host, this.port);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ContactPoint that = (ContactPoint) o;
        return this.port == that.getPort() && Objects.equals(this.host, that.getHost());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.host, this.port);
    }

}
