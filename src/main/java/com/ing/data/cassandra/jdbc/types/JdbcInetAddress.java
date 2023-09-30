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

package com.ing.data.cassandra.jdbc.types;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Types;

/**
 * JDBC description of {@code INET} CQL type (corresponding Java type: {@link InetAddress}).
 * <p>CQL type description: IP address string in IPv4 or IPv6 format, used by the python-cql driver and CQL native
 * protocols.</p>
 */
public class JdbcInetAddress extends AbstractJdbcType<InetAddress> {

    /**
     * Gets a {@code JdbcInetAddress} instance.
     */
    public static final JdbcInetAddress INSTANCE = new JdbcInetAddress();

    // The maximal size for IPv4 is 15 characters ('xxx.xxx.xxx.xxx').
    // The maximal size for IPv4 is 39 characters ('xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx').
    // 'inet' type accepts either IPv4 (4 bytes long) or IPv6 (16 bytes long).
    private static final int DEFAULT_INET_PRECISION = 39;

    JdbcInetAddress() {
    }

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public int getScale(final InetAddress obj) {
        return DEFAULT_SCALE;
    }

    @Override
    public int getPrecision(final InetAddress obj) {
        if (obj != null) {
            return obj.toString().length();
        }
        return DEFAULT_INET_PRECISION;
    }

    @Override
    public boolean isCurrency() {
        return false;
    }

    @Override
    public boolean isSigned() {
        return true;
    }

    @Override
    public String toString(final InetAddress obj) {
        if (obj != null) {
            return obj.getHostAddress();
        } else {
            return null;
        }
    }

    @Override
    public boolean needsQuotes() {
        return false;
    }

    public String getString(final ByteBuffer bytes) {
        return compose(bytes).getHostAddress();
    }

    @Override
    public Class<InetAddress> getType() {
        return InetAddress.class;
    }

    @Override
    public int getJdbcType() {
        return Types.OTHER;
    }

    @Override
    public InetAddress compose(final Object value) {
        try {
            return InetAddress.getByName((String) value);
        } catch (final UnknownHostException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public Object decompose(final InetAddress value) {
        return value.getAddress();
    }

}
