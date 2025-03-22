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

package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.DateCodec;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.ing.data.cassandra.jdbc.utils.ByteBufferUtil;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.time.LocalDate;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

/**
 * Manages the two-way conversion between the CQL type {@link DataTypes#DATE} and the Java type {@link java.sql.Date}.
 *
 * @implNote The CQL type {@code DATE} is stored as an unsigned 32-bit integer.
 */
public class SqlDateCodec extends AbstractCodec<Date> implements TypeCodec<Date> {

    /**
     * Constructor for {@code SqlDateCodec}.
     */
    public SqlDateCodec() {
    }

    @Nonnull
    @Override
    public GenericType<Date> getJavaType() {
        return GenericType.of(Date.class);
    }

    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.DATE;
    }

    @Override
    public ByteBuffer encode(final Date value, @Nonnull final ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        }
        return ByteBufferUtil.bytes(signedToUnsigned((int) value.toLocalDate().toEpochDay()));
    }

    @Override
    public Date decode(final ByteBuffer bytes, @Nonnull final ProtocolVersion protocolVersion) {
        if (bytes == null) {
            return null;
        }
        // always duplicate the ByteBuffer instance before consuming it!
        final int readInt = ByteBufferUtil.toInt(bytes.duplicate());
        return Date.valueOf(LocalDate.ofEpochDay(unsignedToSigned(readInt)));
    }

    @Override
    Date parseNonNull(@Nonnull final String value) {
        // Re-use the parser of the standard DateCodec to handle the formats supported by Cassandra.
        final LocalDate parsedLocalDate = new DateCodec().parse(value);
        if (parsedLocalDate == null) {
            return null;
        }
        return Date.valueOf(parsedLocalDate);
    }

    @Override
    String formatNonNull(@Nonnull final Date value) {
        return Strings.quote(ISO_LOCAL_DATE.format(value.toLocalDate()));
    }

    static int signedToUnsigned(final int signed) {
        return signed - Integer.MIN_VALUE;
    }

    static int unsignedToSigned(final int unsigned) {
        return unsigned + Integer.MIN_VALUE;
    }

}
