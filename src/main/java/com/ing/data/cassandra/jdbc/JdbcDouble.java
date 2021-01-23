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
 * JDBC description of {@code DOUBLE} CQL type (corresponding Java type: {@link Double}).
 * <p>CQL type description: 64-bit IEEE-754 floating point.</p>
 */
public class JdbcDouble extends AbstractJdbcType<Double> {

    private static final int DOUBLE_SCALE = 300;
    private static final int DOUBLE_PRECISION = 300;

    /**
     * Gets a {@code JdbcDouble} instance.
     */
    public static final JdbcDouble instance = new JdbcDouble();

    JdbcDouble() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(final Double obj) {
        return DOUBLE_SCALE;
    }

    public int getPrecision(final Double obj) {
        return DOUBLE_PRECISION;
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return true;
    }

    public String toString(final Double obj) {
        if (obj != null) {
            return obj.toString();
        } else {
            return null;
        }
    }

    public boolean needsQuotes() {
        return false;
    }

    public Class<Double> getType() {
        return Double.class;
    }

    public int getJdbcType() {
        return Types.DOUBLE;
    }

    public Double compose(final Object value) {
        return (Double) value;
    }

    public Object decompose(final Double value) {
        return value;
    }

}
