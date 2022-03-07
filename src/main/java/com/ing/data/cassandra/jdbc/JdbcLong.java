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

    /**
     * Gets a {@code JdbcLong} instance.
     */
    public static final JdbcLong INSTANCE = new JdbcLong();

    // The maximal size of a 64-bit signed integer is 20 (length of '-9223372036854775808').
    private static final int DEFAULT_LONG_PRECISION = 20;

    JdbcLong() {
    }

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public int getScale(final Long obj) {
        return DEFAULT_SCALE;
    }

    @Override
    public int getPrecision(final Long obj) {
        if (obj != null) {
            return obj.toString().length();
        }
        return DEFAULT_LONG_PRECISION;
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
    public String toString(final Long obj) {
        if (obj != null) {
            return obj.toString();
        } else {
            return null;
        }
    }

    @Override
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

    @Override
    public Class<Long> getType() {
        return Long.class;
    }

    @Override
    public int getJdbcType() {
        return Types.BIGINT;
    }

    @Override
    public Long compose(final Object obj) {
        return (Long) obj;
    }

    @Override
    public Object decompose(final Long value) {
        return value;
    }

}
