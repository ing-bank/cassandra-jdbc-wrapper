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
 * JDBC description of {@code INT} CQL type (corresponding Java type: {@link Integer}).
 * <p>CQL type description: 32-bit signed integer.</p>
 */
public class JdbcInt32 extends AbstractJdbcType<Integer> {

    /**
     * Gets a {@code JdbcInt32} instance.
     */
    public static final JdbcInt32 INSTANCE = new JdbcInt32();

    // The maximal size of a 32-bit signed integer is 11 (length of '-2147483648').
    private static final int DEFAULT_INT_PRECISION = 11;

    JdbcInt32() {
    }

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public int getScale(final Integer obj) {
        return DEFAULT_SCALE;
    }

    @Override
    public int getPrecision(final Integer obj) {
        if (obj != null) {
            return obj.toString().length();
        }
        return DEFAULT_INT_PRECISION;
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
    public String toString(final Integer obj) {
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
    public Class<Integer> getType() {
        return Integer.class;
    }

    @Override
    public int getJdbcType() {
        return Types.INTEGER;
    }

    @Override
    public Integer compose(final Object value) {
        return (Integer) value;
    }

    @Override
    public Object decompose(final Integer value) {
        return value;
    }

}
