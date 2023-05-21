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

import java.sql.Types;

/**
 * JDBC description of {@code TINYINT} CQL type (corresponding Java type: {@link Byte}).
 * <p>CQL type description: 1 byte signed integer.</p>
 */
public class JdbcByte extends AbstractJdbcType<Byte> {

    /**
     * Gets a {@code JdbcByte} instance.
     */
    public static final JdbcByte INSTANCE = new JdbcByte();

    // The maximal size of an 8-bit signed integer is 4 (length of '-128').
    private static final int DEFAULT_TINYINT_PRECISION = 4;

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public int getScale(final Byte obj) {
        return DEFAULT_SCALE;
    }

    @Override
    public int getPrecision(final Byte obj) {
        if (obj != null) {
            return obj.toString().length();
        }
        return DEFAULT_TINYINT_PRECISION;
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
    public String toString(final Byte obj) {
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

    @Override
    public Class<Byte> getType() {
        return Byte.class;
    }

    @Override
    public int getJdbcType() {
        return Types.TINYINT;
    }

    @Override
    public Byte compose(final Object value) {
        return (Byte) value;
    }

    @Override
    public Object decompose(final Byte value) {
        return value;
    }

}
