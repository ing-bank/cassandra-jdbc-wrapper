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

package com.ing.data.cassandra.jdbc.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.util.Calendar;

import static com.ing.data.cassandra.jdbc.utils.WarningConstants.BINARY_FAILED_CONVERSION;
import static org.apache.commons.io.IOUtils.EMPTY_BYTE_ARRAY;
import static org.apache.commons.io.IOUtils.toByteArray;

/**
 * A set of static utility methods for types conversions.
 */
@Slf4j
public final class ConversionsUtil {

    private ConversionsUtil() {
        // Private constructor to hide the public one.
    }

    /**
     * Converts an object of one these types to a byte array for storage in a column of type {@code blob}:
     * <ul>
     *     <li>{@code byte[]}</li>
     *     <li>{@link ByteArrayInputStream}</li>
     *     <li>{@link Blob}</li>
     *     <li>{@link Clob}</li>
     *     <li>{@link NClob} (this is handled as {@link Clob} since it's just a superinterface of this one)</li>
     * </ul>
     *
     * @param x The object to convert.
     * @return The byte array resulting of the object conversion. An empty array is returned if the type is valid but
     * the conversion failed for some reason (see the logged error for further details).
     * @throws SQLException when the type of the object to convert is not supported.
     */
    public static byte[] convertToByteArray(final Object x) throws SQLException {
        if (x instanceof ByteArrayInputStream byteArrayInputStream) {
            try {
                return toByteArray(byteArrayInputStream, byteArrayInputStream.available());
            } catch (final IOException e) {
                return onBinaryFailedConversion(e, x);
            }
        } else if (x instanceof byte[] byteArray) {
            return byteArray;
        } else if (x instanceof Blob blob) {
            try {
                final InputStream stream = blob.getBinaryStream();
                return toByteArray(stream, stream.available());
            } catch (final IOException | SQLException e) {
                return onBinaryFailedConversion(e, x);
            }
        } else if (x instanceof Clob clob) {
            try (Reader reader = clob.getCharacterStream()) {
                return toByteArray(reader, StandardCharsets.UTF_8);
            } catch (final IOException | SQLException e) {
                return onBinaryFailedConversion(e, x);
            }
        } else {
            throw new SQLException(String.format(ErrorConstants.UNSUPPORTED_PARAMETER_TYPE, x.getClass()));
        }
    }

    /**
     * Converts an object of one these types to a {@link LocalDate} for storage in a column of type {@code date}:
     * <ul>
     *     <li>{@link LocalDate}</li>
     *     <li>{@link Date}</li>
     * </ul>
     *
     * @param x The object to convert.
     * @return The {@link LocalDate} instance resulting of the object conversion.
     * @throws SQLException when the type of the object to convert is not supported.
     */
    public static LocalDate convertToLocalDate(final Object x) throws SQLException {
        if (x instanceof LocalDate localDate) {
            return localDate;
        } else if (x instanceof java.sql.Date date) {
            return date.toLocalDate();
        } else {
            throw new SQLException(String.format(ErrorConstants.UNSUPPORTED_PARAMETER_TYPE, x.getClass()));
        }
    }

    /**
     * Converts an object of one these types to a {@link LocalTime} for storage in a column of type {@code time}:
     * <ul>
     *     <li>{@link LocalTime}</li>
     *     <li>{@link Time}</li>
     * </ul>
     *
     * @param x The object to convert.
     * @return The {@link LocalTime} instance resulting of the object conversion.
     * @throws SQLException when the type of the object to convert is not supported.
     */
    public static LocalTime convertToLocalTime(final Object x) throws SQLException {
        if (x instanceof LocalTime localTime) {
            return localTime;
        } else if (x instanceof java.sql.Time time) {
            return time.toLocalTime();
        } else if (x instanceof OffsetTime offsetTime) {
            return offsetTime.toLocalTime();
        } else {
            throw new SQLException(String.format(ErrorConstants.UNSUPPORTED_PARAMETER_TYPE, x.getClass()));
        }
    }

    /**
     * Converts an object of one these types to a {@link Instant} for storage in a column of type {@code timestamp}:
     * <ul>
     *     <li>{@link LocalDateTime}</li>
     *     <li>{@link Timestamp}</li>
     *     <li>{@link java.util.Date}</li>
     *     <li>{@link Calendar}</li>
     *     <li>{@link OffsetDateTime}</li>
     * </ul>
     *
     * @param x The object to convert.
     * @return The {@link LocalTime} instance resulting of the object conversion.
     * @throws SQLException when the type of the object to convert is not supported.
     */
    public static Instant convertToInstant(final Object x) throws SQLException {
        if (x instanceof LocalDateTime localDateTime) {
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant();
        } else if (x instanceof java.sql.Timestamp timestamp) {
            return timestamp.toInstant();
        } else if (x instanceof java.util.Date date) {
            return date.toInstant();
        } else if (x instanceof Calendar calendar) {
            return calendar.toInstant();
        } else if (x instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toInstant();
        } else {
            throw new SQLException(String.format(ErrorConstants.UNSUPPORTED_PARAMETER_TYPE, x.getClass()));
        }
    }

    private static byte[] onBinaryFailedConversion(final Exception e, final Object obj) {
        log.warn(BINARY_FAILED_CONVERSION, obj.getClass().getName(), e);
        return EMPTY_BYTE_ARRAY;
    }
}
