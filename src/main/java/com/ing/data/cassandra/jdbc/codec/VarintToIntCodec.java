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
import com.ing.data.cassandra.jdbc.utils.ByteBufferUtil;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

/**
 * Manages the two-way conversion between the CQL type {@link DataTypes#VARINT} and the Java type {@link Integer}.
 */
public class VarintToIntCodec extends AbstractCodec<Integer> implements TypeCodec<Integer> {

    /**
     * Constructor for {@code VarintToIntCodec}.
     */
    public VarintToIntCodec() {
        super();
    }

    @Nonnull
    @Override
    public GenericType<Integer> getJavaType() {
        return GenericType.INTEGER;
    }

    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.VARINT;
    }

    @Override
    public ByteBuffer encode(final Integer value, @Nonnull final ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        }
        return ByteBufferUtil.bytes(value);
    }

    @Override
    public Integer decode(final ByteBuffer bytes, @Nonnull final ProtocolVersion protocolVersion) {
        if (bytes == null) {
            return null;
        }
        // always duplicate the ByteBuffer instance before consuming it!
        return ByteBufferUtil.toInt(bytes.duplicate());
    }

    @Override
    Integer parseNonNull(@Nonnull final String value) {
        return Integer.valueOf(value);
    }

    @Override
    String formatNonNull(@Nonnull final Integer value) {
        return String.valueOf(value);
    }

}
