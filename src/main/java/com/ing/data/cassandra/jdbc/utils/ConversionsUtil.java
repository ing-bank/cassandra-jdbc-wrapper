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

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * A set of static utility methods for types conversions.
 */
public final class ConversionsUtil {

    static final Logger LOG = LoggerFactory.getLogger(ConversionsUtil.class);

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
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static byte[] convertToByteArray(final Object x) throws SQLException {
        byte[] array = new byte[0];
        if (x instanceof ByteArrayInputStream) {
            array = new byte[((ByteArrayInputStream) x).available()];
            try {
                ((ByteArrayInputStream) x).read(array);
            } catch (final IOException e) {
                LOG.warn(BINARY_FAILED_CONVERSION, x.getClass().getName(), e);
            }
        } else if (x instanceof byte[]) {
            array = (byte[]) x;
        } else if (x instanceof Blob) {
            try {
                final InputStream stream = ((Blob) x).getBinaryStream();
                array = new byte[stream.available()];
                stream.read(array);
            } catch (final IOException | SQLException e) {
                LOG.warn(BINARY_FAILED_CONVERSION, x.getClass().getName(), e);
            }
        } else if (x instanceof Clob) {
            try (Reader reader = ((Clob) x).getCharacterStream()) {
                array = IOUtils.toByteArray(reader, StandardCharsets.UTF_8);
            } catch (final IOException | SQLException e) {
                LOG.warn(BINARY_FAILED_CONVERSION, x.getClass().getName(), e);
            }
        } else {
            throw new SQLException(String.format(ErrorConstants.UNSUPPORTED_PARAMETER_TYPE, x.getClass()));
        }
        return array;
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
        if (x instanceof LocalDate) {
            return (LocalDate) x;
        } else if (x instanceof java.sql.Date) {
            return ((Date) x).toLocalDate();
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
        if (x instanceof LocalTime) {
            return (LocalTime) x;
        } else if (x instanceof java.sql.Time) {
            return ((Time) x).toLocalTime();
        } else if (x instanceof OffsetTime) {
            return ((OffsetTime) x).toLocalTime();
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
        if (x instanceof LocalDateTime) {
            return ((LocalDateTime) x).atZone(ZoneId.systemDefault()).toInstant();
        } else if (x instanceof java.sql.Timestamp) {
            return ((Timestamp) x).toInstant();
        } else if (x instanceof java.util.Date) {
            return ((java.util.Date) x).toInstant();
        } else if (x instanceof Calendar) {
            return ((Calendar) x).toInstant();
        } else if (x instanceof OffsetDateTime) {
            return ((OffsetDateTime) x).toInstant();
        } else {
            throw new SQLException(String.format(ErrorConstants.UNSUPPORTED_PARAMETER_TYPE, x.getClass()));
        }
    }
}
