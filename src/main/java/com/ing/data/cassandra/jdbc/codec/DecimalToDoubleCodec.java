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
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;

public class DecimalToDoubleCodec implements TypeCodec<Double> {

    public DecimalToDoubleCodec() {
    }

    @NonNull
    @Override
    public GenericType<Double> getJavaType() {
        return GenericType.DOUBLE;
    }

    @NonNull
    @Override
    public DataType getCqlType() {
        return DataTypes.DECIMAL;
    }

    @Override
    public ByteBuffer encode(final Double paramT, @NonNull final ProtocolVersion paramProtocolVersion) {
        if (paramT == null) {
            return null;
        }
        return ByteBufferUtil.bytes(paramT);
    }

    @Override
    public Double decode(final ByteBuffer paramByteBuffer, @NonNull final ProtocolVersion paramProtocolVersion) {
        if (paramByteBuffer == null) {
            return null;

        }
        // always duplicate the ByteBuffer instance before consuming it!
        final float value = ByteBufferUtil.toFloat(paramByteBuffer.duplicate());
        return (double) value;
    }

    @Override
    public Double parse(final String paramString) {
        return Double.valueOf(paramString);
    }

    @NonNull
    @Override
    public String format(final Double paramT) {
        return String.valueOf(paramT);
    }

}
