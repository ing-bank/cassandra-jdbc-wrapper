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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Types;

import static com.ing.data.cassandra.jdbc.utils.DriverUtil.NULL_KEYWORD;

/**
 * JDBC description of {@code DECIMAL} CQL type (corresponding Java type: {@link BigDecimal}).
 * <p>CQL type description: variable-precision decimal.</p>
 */
public class JdbcDecimal extends AbstractJdbcType<BigDecimal> {

    /**
     * Gets a {@code JdbcDecimal} instance.
     */
    public static final JdbcDecimal INSTANCE = new JdbcDecimal();

    private static final int DEFAULT_DECIMAL_PRECISION = 0;

    JdbcDecimal() {
    }

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public int getScale(final BigDecimal obj) {
        if (obj != null) {
            return obj.scale();
        }
        return DEFAULT_SCALE;
    }

    @Override
    public int getPrecision(final BigDecimal obj) {
        if (obj != null) {
            return obj.precision();
        }
        return DEFAULT_DECIMAL_PRECISION;
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
    public String toString(final BigDecimal obj) {
        if (obj != null) {
            return obj.toPlainString();
        } else {
            return null;
        }
    }

    @Override
    public boolean needsQuotes() {
        return false;
    }

    public String getString(final ByteBuffer bytes) {
        if (bytes == null) {
            return NULL_KEYWORD;
        }
        if (bytes.remaining() == 0) {
            return "empty";
        }
        return compose(bytes).toPlainString();
    }

    @Override
    public Class<BigDecimal> getType() {
        return BigDecimal.class;
    }

    @Override
    public int getJdbcType() {
        return Types.DECIMAL;
    }

    @Override
    public BigDecimal compose(final Object value) {
        return (BigDecimal) value;
    }

    @Override
    public Object decompose(final BigDecimal value) {
        // The bytes of the ByteBuffer are made up of 4 bytes of int containing the scale followed by the n bytes it
        // takes to store a BigInteger.
        return value;
    }

}
