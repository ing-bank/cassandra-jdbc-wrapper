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

import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.sql.Types;

/**
 * JDBC description of {@code BLOB} CQL type (corresponding Java type: {@link ByteBuffer}).
 * <p>CQL type description: arbitrary bytes (no validation), expressed as hexadecimal.</p>
 */
public class JdbcBytes extends AbstractJdbcType<ByteBuffer> {

    /**
     * Gets a {@code JdbcBytes} instance.
     */
    public static final JdbcBytes INSTANCE = new JdbcBytes();

    JdbcBytes() {
    }

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public int getScale(final ByteBuffer obj) {
        return DEFAULT_SCALE;
    }

    @Override
    public int getPrecision(final ByteBuffer obj) {
        if (obj != null) {
            return obj.remaining();
        }
        return Integer.MAX_VALUE / 2;
    }

    @Override
    public boolean isCurrency() {
        return false;
    }

    @Override
    public boolean isSigned() {
        return false;
    }

    @Override
    public String toString(final ByteBuffer obj) {
        return StringUtils.EMPTY;
    }

    @Override
    public boolean needsQuotes() {
        return true;
    }

    @Override
    public Class<ByteBuffer> getType() {
        return ByteBuffer.class;
    }

    @Override
    public int getJdbcType() {
        return Types.BINARY;
    }

    /**
     * Duplicates the given {@link ByteBuffer}.
     *
     * @param bytes The {@link ByteBuffer} instance.
     * @return A duplicated instance of the given {@link ByteBuffer}.
     */
    public ByteBuffer compose(final ByteBuffer bytes) {
        return bytes.duplicate();
    }

    @Override
    public ByteBuffer compose(final Object obj) {
        return null;
    }

    @Override
    public ByteBuffer decompose(final ByteBuffer value) {
        return value;
    }

}
