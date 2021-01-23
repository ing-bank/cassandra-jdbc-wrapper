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
 * JDBC description of {@code BLOB} CQL type (corresponding Java type: {@link ByteBuffer}).
 * <p>CQL type description: arbitrary bytes (no validation), expressed as hexadecimal.</p>
 */
public class JdbcBytes extends AbstractJdbcType<ByteBuffer> {

    /**
     * Gets a {@code JdbcBytes} instance.
     */
    public static final JdbcBytes instance = new JdbcBytes();

    JdbcBytes() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(final ByteBuffer obj) {
        return DEFAULT_SCALE;
    }

    public int getPrecision(final ByteBuffer obj) {
        if (obj != null) {
            return obj.remaining();
        }
        return Integer.MAX_VALUE / 2;
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return false;
    }

    public String toString(final ByteBuffer obj) {
        return StringUtils.EMPTY;
    }

    public boolean needsQuotes() {
        return true;
    }

    public Class<ByteBuffer> getType() {
        return ByteBuffer.class;
    }

    public int getJdbcType() {
        return Types.BINARY;
    }

    public ByteBuffer compose(final ByteBuffer bytes) {
        return bytes.duplicate();
    }

    public ByteBuffer decompose(final ByteBuffer value) {
        return value;
    }

    @Override
    public ByteBuffer compose(final Object obj) {
        return null;
    }

}
