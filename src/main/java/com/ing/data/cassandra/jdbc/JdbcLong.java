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

import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.sql.Types;

/**
 * JDBC description of {@code BIGINT} CQL type (corresponding Java type: {@link Long}).
 * <p>CQL type description: 64-bit signed integer.</p>
 */
public class JdbcLong extends AbstractJdbcType<Long> {

    private static final int LONG_SCALE = 0;

    /**
     * Gets a {@code JdbcLong} instance.
     */
    public static final JdbcLong instance = new JdbcLong();

    JdbcLong() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(final Long obj) {
        return LONG_SCALE;
    }

    public int getPrecision(final Long obj) {
        return obj.toString().length();
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return true;
    }

    public String toString(final Long obj) {
        return obj.toString();
    }

    public boolean needsQuotes() {
        return false;
    }

    public String getString(final ByteBuffer bytes) {
        if (bytes.remaining() == 0) {
            return StringUtils.EMPTY;
        }
        if (bytes.remaining() != 8) {
            throw new MarshalException("A long is exactly 8 bytes, but found: " + bytes.remaining());
        }
        return String.valueOf(bytes.getLong(bytes.position()));
    }

    public Class<Long> getType() {
        return Long.class;
    }

    public int getJdbcType() {
        return Types.BIGINT;
    }

    public Long compose(final Object obj) {
        return (Long) obj;
    }

    public Object decompose(final Long value) {
        return value;
    }

}
