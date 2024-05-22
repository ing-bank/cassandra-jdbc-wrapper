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

import java.sql.Timestamp;
import java.sql.Types;
import java.time.format.DateTimeFormatter;

/**
 * JDBC description of {@code TIMESTAMP} CQL type (corresponding Java type: {@link Timestamp}).
 * <p>CQL type description: date and time with millisecond precision, encoded as 8 bytes since epoch. Timestamps can be
 * represented as a string, such as 2015-05-03 13:30:54.234 (yyyy-MM-dd hh:mm:ss.SSS).</p>
 */
public class JdbcTimestamp extends AbstractJdbcType<Timestamp> {
    private static final int TIMESTAMP_SCALE = 3;

    /**
     * Gets a {@code JdbcTimestamp} instance.
     */
    public static final JdbcTimestamp INSTANCE = new JdbcTimestamp();

    // The maximal size of timestamp is 31 (for the format 'yyyy-MM-ddThh:mm:ss.SSSSSS[+-]xxxx').
    private static final int DEFAULT_TIMESTAMP_PRECISION = 31;

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public int getScale(final Timestamp obj) {
        return TIMESTAMP_SCALE;
    }

    @Override
    public int getPrecision(final Timestamp obj) {
        if (obj != null) {
            return toString(obj).length();
        }
        return DEFAULT_TIMESTAMP_PRECISION;
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
    public String toString(final Timestamp obj) {
        if (obj != null) {
            return obj.toLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } else {
            return null;
        }
    }

    @Override
    public boolean needsQuotes() {
        return false;
    }

    @Override
    public Class<Timestamp> getType() {
        return Timestamp.class;
    }

    @Override
    public int getJdbcType() {
        return Types.TIMESTAMP;
    }

    @Override
    public Timestamp compose(final Object value) {
        return (Timestamp) value;
    }

    @Override
    public Object decompose(final Timestamp value) {
        return value;
    }

}
