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
import edu.umd.cs.findbugs.annotations.NonNull;
import com.ing.data.cassandra.jdbc.ByteBufferUtil;

import java.nio.ByteBuffer;

/**
 * Manages the two-way conversion between the CQL type {@link DataTypes#TIMESTAMP} and the Java type {@link Long}.
 */
public class TimestampToLongCodec extends AbstractCodec<Long> implements TypeCodec<Long> {

    public TimestampToLongCodec() {
    }

    @NonNull
    @Override
    public GenericType<Long> getJavaType() {
        return GenericType.LONG;
    }

    @NonNull
    @Override
    public DataType getCqlType() {
        return DataTypes.TIMESTAMP;
    }

    @Override
    public ByteBuffer encode(final Long value, @NonNull final ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        }
        return ByteBufferUtil.bytes(value);
    }

    @Override
    public Long decode(final ByteBuffer bytes, @NonNull final ProtocolVersion protocolVersion) {
        if (bytes == null) {
            return null;
        }
        // always duplicate the ByteBuffer instance before consuming it!
        return ByteBufferUtil.toLong(bytes.duplicate());
    }

    @Override
    Long parseNonNull(@NonNull final String value) {
        return Long.valueOf(value);
    }

    @Override
    String formatNonNull(@NonNull final Long value) {
        return String.valueOf(value);
    }

}
