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

import jakarta.annotation.Nonnull;
import java.nio.ByteBuffer;

/**
 * Manages the two-way conversion between the CQL type {@link DataTypes#DECIMAL} and the Java type {@link Double}.
 */
public class DecimalToDoubleCodec extends AbstractCodec<Double> implements TypeCodec<Double> {

    /**
     * Constructor for {@code DecimalToDoubleCodec}.
     */
    public DecimalToDoubleCodec() {
    }

    @Nonnull
    @Override
    public GenericType<Double> getJavaType() {
        return GenericType.DOUBLE;
    }

    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.DECIMAL;
    }

    @Override
    public ByteBuffer encode(final Double value, @Nonnull final ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        }
        return ByteBufferUtil.bytes(value);
    }

    @Override
    public Double decode(final ByteBuffer bytes, @Nonnull final ProtocolVersion protocolVersion) {
        if (bytes == null) {
            return null;
        }
        // always duplicate the ByteBuffer instance before consuming it!
        return ByteBufferUtil.toDouble(bytes.duplicate());
    }

    @Override
    Double parseNonNull(@Nonnull final String value) {
        return Double.valueOf(value);
    }

    @Override
    String formatNonNull(@Nonnull final Double value) {
        return String.valueOf(value);
    }

}
