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

import java.math.BigInteger;
import java.sql.Types;

/**
 * JDBC description of {@code VARINT} CQL type (corresponding Java type: {@link BigInteger}).
 * <p>CQL type description: 64-bit signed integer.</p>
 */
public class JdbcInteger extends AbstractJdbcType<BigInteger> {

    // The maximal size of a 64-bit signed integer is 20 (length of '-9223372036854775808').
    private static final int DEFAULT_INTEGER_PRECISION = 20;

    /**
     * Gets a {@code JdbcInteger} instance.
     */
    public static final JdbcInteger instance = new JdbcInteger();

    JdbcInteger() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(final BigInteger obj) {
        return DEFAULT_SCALE;
    }

    public int getPrecision(final BigInteger obj) {
        if (obj != null) {
            return obj.toString().length();
        }
        return DEFAULT_INTEGER_PRECISION;
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return true;
    }

    public String toString(final BigInteger obj) {
        if (obj != null) {
            return obj.toString();
        } else {
            return null;
        }
    }

    public boolean needsQuotes() {
        return false;
    }

    public Class<BigInteger> getType() {
        return BigInteger.class;
    }

    public int getJdbcType() {
        return Types.BIGINT;
    }

    public BigInteger compose(final Object obj) {
        return (BigInteger) obj;
    }

    public Object decompose(final BigInteger value) {
        return value;
    }

}
