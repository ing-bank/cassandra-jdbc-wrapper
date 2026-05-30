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

import static com.ing.data.cassandra.jdbc.utils.DriverUtil.toStringOrNull;

/**
 * JDBC description of {@code SMALLINT} CQL type (corresponding Java type: {@link Short}).
 * <p>CQL type description: 2 byte signed integer.</p>
 */
public class JdbcShort extends AbstractJdbcType<Short> {

    /**
     * Gets a {@code JdbcShort} instance.
     */
    public static final JdbcShort INSTANCE = new JdbcShort();

    // The maximal size of a 16-bit signed integer is 6 (length of '-32768').
    private static final int DEFAULT_SMALLINT_PRECISION = 6;

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public int getScale(final Short obj) {
        return DEFAULT_SCALE;
    }

    @Override
    public int getPrecision(final Short obj) {
        if (obj != null) {
            return obj.toString().length();
        }
        return DEFAULT_SMALLINT_PRECISION;
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
    public String toString(final Short obj) {
        return toStringOrNull(obj);
    }

    @Override
    public boolean needsQuotes() {
        return false;
    }

    @Override
    public Class<Short> getType() {
        return Short.class;
    }

    @Override
    public int getJdbcType() {
        return Types.SMALLINT;
    }

    @Override
    public Short compose(final Object value) {
        return (Short) value;
    }

    @Override
    public Object decompose(final Short value) {
        return value;
    }

}
