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

public class TimestampToLongCodec implements TypeCodec<Long> {

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
    public ByteBuffer encode(final Long paramT, @NonNull final ProtocolVersion paramProtocolVersion) {
        if (paramT == null) {
            return null;
        }
        return ByteBufferUtil.bytes(paramT);
    }

    @Override
    public Long decode(final ByteBuffer paramByteBuffer, @NonNull final ProtocolVersion paramProtocolVersion) {
        if (paramByteBuffer == null) {
            return null;

        }
        // always duplicate the ByteBuffer instance before consuming it!
        return ByteBufferUtil.toLong(paramByteBuffer.duplicate());
    }

    @Override
    public Long parse(final String paramString) {
        return Long.valueOf(paramString);
    }

    @NonNull
    @Override
    public String format(final Long paramT) {
        return String.valueOf(paramT);
    }

}
