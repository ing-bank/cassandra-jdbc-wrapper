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

import java.sql.Types;

/**
 * JDBC description of {@code INT} CQL type (corresponding Java type: {@link Integer}).
 * <p>CQL type description: 32-bit signed integer.</p>
 */
public class JdbcInt32 extends AbstractJdbcType<Integer> {

    private static final int INT_SCALE = 0;

    /**
     * Gets a {@code JdbcInt32} instance.
     */
    public static final JdbcInt32 instance = new JdbcInt32();

    JdbcInt32() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(final Integer obj) {
        return INT_SCALE;
    }

    public int getPrecision(final Integer obj) {
        return obj.toString().length();
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return true;
    }

    public String toString(final Integer obj) {
        return obj.toString();
    }

    public boolean needsQuotes() {
        return false;
    }

    public Class<Integer> getType() {
        return Integer.class;
    }

    public int getJdbcType() {
        return Types.INTEGER;
    }

    public Integer compose(final Object value) {
        return (Integer) value;
    }

    public Object decompose(final Integer value) {
        return value;
    }

}
