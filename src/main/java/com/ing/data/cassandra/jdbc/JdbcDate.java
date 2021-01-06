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

import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * JDBC description of {@code DATE} CQL type (corresponding Java type: {@link Date}).
 * <p>CQL type description: a date with no corresponding time value; Cassandra encodes date as a 32-bit integer
 * representing days since epoch (January 1, 1970). Dates can be represented in queries and inserts as a string,
 * such as 2015-05-03 (yyyy-mm-dd).</p>
 */
public class JdbcDate extends AbstractJdbcType<Date> {

    /**
     * Valid ISO-8601 patterns for date type.
     */
    public static final String[] iso8601Patterns = new String[]{
        "yyyy-MM-dd HH:mm",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mmZ",
        "yyyy-MM-dd HH:mm:ssZ",
        "yyyy-MM-dd'T'HH:mm",
        "yyyy-MM-dd'T'HH:mmZ",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ssZ",
        "yyyy-MM-dd",
        "yyyy-MM-ddZ",
        "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss.SSS",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    };
    /**
     * Default date format: yyyy-MM-dd HH:mm:ssZ.
     */
    static final String DEFAULT_FORMAT = iso8601Patterns[3];
    static final ThreadLocal<SimpleDateFormat> FORMATTER = ThreadLocal.withInitial(
        () -> new SimpleDateFormat(DEFAULT_FORMAT));

    /**
     * Gets a {@code JdbcDate} instance.
     */
    public static final JdbcDate instance = new JdbcDate();

    JdbcDate() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(final Date obj) {
        return DEFAULT_SCALE;
    }

    public int getPrecision(final Date obj) {
        return DEFAULT_PRECISION;
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return false;
    }

    public String toString(final Date obj) {
        return FORMATTER.get().format(obj);
    }

    public boolean needsQuotes() {
        return false;
    }

    public String getString(final ByteBuffer bytes) {
        if (bytes.remaining() == 0) {
            return StringUtils.EMPTY;
        }
        if (bytes.remaining() != 8) {
            throw new MarshalException("A date is exactly 8 bytes (stored as a long), but found: " + bytes.remaining());
        }
        // Use ISO-8601 formatted string.
        return FORMATTER.get().format(new Date(bytes.getLong(bytes.position())));
    }

    public Class<Date> getType() {
        return Date.class;
    }

    public int getJdbcType() {
        return Types.TIMESTAMP;
    }

    public Date compose(final Object value) {
        return (Date) value;
    }

    public Object decompose(final Date value) {
        return value;
    }

}
