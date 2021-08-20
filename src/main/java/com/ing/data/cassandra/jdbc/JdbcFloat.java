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
 * JDBC description of {@code FLOAT} CQL type (corresponding Java type: {@link Float}).
 * <p>CQL type description: 32-bit IEEE-754 floating point.</p>
 */
public class JdbcFloat extends AbstractJdbcType<Float> {

    /**
     * Gets a {@code JdbcFloat} instance.
     */
    public static final JdbcFloat INSTANCE = new JdbcFloat();

    private static final int FLOAT_SCALE = 40;
    private static final int FLOAT_PRECISION = 7;

    JdbcFloat() {
    }

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public int getScale(final Float obj) {
        return FLOAT_SCALE;
    }

    @Override
    public int getPrecision(final Float obj) {
        return FLOAT_PRECISION;
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
    public String toString(final Float obj) {
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
    public Class<Float> getType() {
        return Float.class;
    }

    @Override
    public int getJdbcType() {
        return Types.FLOAT;
    }

    @Override
    public Float compose(final Object value) {
        return (Float) value;
    }

    @Override
    public Object decompose(final Float value) {
        return value;
    }

}
