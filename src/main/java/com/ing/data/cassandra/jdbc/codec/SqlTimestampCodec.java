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
import com.datastax.oss.driver.internal.core.type.codec.TimestampCodec;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.ing.data.cassandra.jdbc.utils.ByteBufferUtil;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Manages the two-way conversion between the CQL type {@link DataTypes#TIMESTAMP} and the Java type {@link Timestamp}.
 */
public class SqlTimestampCodec extends AbstractCodec<Timestamp> implements TypeCodec<Timestamp> {

    /**
     * The default timestamp format used by the method {@link #formatNonNull(Timestamp)}.
     */
    public static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneId.systemDefault());

    /**
     * Constructor for {@code SqlTimestampCodec}.
     */
    public SqlTimestampCodec() {
    }

    @Nonnull
    @Override
    public GenericType<Timestamp> getJavaType() {
        return GenericType.of(Timestamp.class);
    }

    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.TIMESTAMP;
    }

    @Override
    public ByteBuffer encode(final Timestamp value, @Nonnull final ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        }
        return ByteBufferUtil.bytes(value.toInstant().toEpochMilli());
    }

    @Override
    public Timestamp decode(final ByteBuffer bytes, @Nonnull final ProtocolVersion protocolVersion) {
        if (bytes == null) {
            return null;
        }
        // always duplicate the ByteBuffer instance before consuming it!
        return new Timestamp(ByteBufferUtil.toLong(bytes.duplicate()));
    }

    @Override
    Timestamp parseNonNull(@Nonnull final String value) {
        // Re-use the parser of the standard TimestampCodec to handle the different formats supported by Cassandra.
        final Instant parsedInstant = new TimestampCodec().parse(value);
        if (parsedInstant == null) {
            return null;
        }
        return Timestamp.from(parsedInstant);
    }

    @Override
    String formatNonNull(@Nonnull final Timestamp value) {
        return Strings.quote(DEFAULT_TIMESTAMP_FORMAT.format(value.toInstant()));
    }

}
