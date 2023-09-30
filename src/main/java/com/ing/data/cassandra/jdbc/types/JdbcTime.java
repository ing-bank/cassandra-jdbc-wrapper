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

import java.sql.Time;
import java.sql.Types;
import java.time.format.DateTimeFormatter;

/**
 * JDBC description of {@code TIME} CQL type (corresponding Java type: {@link Time}).
 * <p>CQL type description: a value encoded as a 64-bit signed integer representing the number of nanoseconds since
 * midnight. Time values can be represented as strings, such as 13:30:54.234 (hh:mm:ss.SSS).</p>
 */
public class JdbcTime extends AbstractJdbcType<Time> {

    /**
     * Gets a {@code JdbcTime} instance.
     */
    public static final JdbcTime INSTANCE = new JdbcTime();

    // The maximal size of time is 18 (for the format 'hh:mm:ss.SSSSSSSSS').
    private static final int DEFAULT_TIME_PRECISION = 18;

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public int getScale(final Time obj) {
        return DEFAULT_SCALE;
    }

    @Override
    public int getPrecision(final Time obj) {
        if (obj != null) {
            return toString(obj).length();
        }
        return DEFAULT_TIME_PRECISION;
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
    public String toString(final Time obj) {
        if (obj != null) {
            return obj.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME);
        } else {
            return null;
        }
    }

    @Override
    public boolean needsQuotes() {
        return false;
    }

    @Override
    public Class<Time> getType() {
        return Time.class;
    }

    @Override
    public int getJdbcType() {
        return Types.TIME;
    }

    @Override
    public Time compose(final Object value) {
        return (Time) value;
    }

    @Override
    public Object decompose(final Time value) {
        return value;
    }

}
