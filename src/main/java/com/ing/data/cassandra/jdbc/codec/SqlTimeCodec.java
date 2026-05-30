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
import com.datastax.oss.driver.internal.core.type.codec.TimeCodec;
import jakarta.annotation.Nonnull;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import static com.datastax.oss.driver.internal.core.util.Strings.quote;
import static com.ing.data.cassandra.jdbc.utils.ByteBufferUtil.bytes;
import static com.ing.data.cassandra.jdbc.utils.ByteBufferUtil.toLong;
import static java.sql.Time.valueOf;

/**
 * Manages the two-way conversion between the CQL type {@link DataTypes#TIME} and the Java type {@link Time}.
 */
public class SqlTimeCodec extends AbstractCodec<Time> implements TypeCodec<Time> {

    /**
     * The default time format used by the method {@link #formatNonNull(Time)}.
     */
    public static final DateTimeFormatter DEFAULT_TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS");

    /**
     * Constructor for {@code SqlTimeCodec}.
     */
    public SqlTimeCodec() {
        super();
    }

    @Nonnull
    @Override
    public GenericType<Time> getJavaType() {
        return GenericType.of(Time.class);
    }

    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.TIME;
    }

    @Override
    public ByteBuffer encode(final Time value, @Nonnull final ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        }
        return bytes(value.toLocalTime().toNanoOfDay());
    }

    @Override
    public Time decode(final ByteBuffer bytes, @Nonnull final ProtocolVersion protocolVersion) {
        if (bytes == null) {
            return null;
        }
        // always duplicate the ByteBuffer instance before consuming it!
        return valueOf(LocalTime.ofNanoOfDay(toLong(bytes.duplicate())));
    }

    @Override
    Time parseNonNull(@Nonnull final String value) {
        // Re-use the parser of the standard TimeCodec to handle the formats supported by Cassandra.
        final LocalTime parsedLocalTime = new TimeCodec().parse(value);
        if (parsedLocalTime == null) {
            return null;
        }
        return valueOf(parsedLocalTime);
    }

    @Override
    String formatNonNull(@Nonnull final Time value) {
        return quote(DEFAULT_TIME_FORMAT.format(value.toLocalTime()));
    }

}
