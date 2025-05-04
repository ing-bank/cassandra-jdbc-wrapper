/*
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
package com.ing.data.cassandra.jdbc.testing;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.ing.data.cassandra.jdbc.utils.ByteBufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Custom codec TEXT <-> java.nio.ByteBuffer for custom codec testing.
 */
public class TextToByteBufferCodec implements TypeCodec<ByteBuffer> {

    @Override
    public @NotNull GenericType<ByteBuffer> getJavaType() {
        return GenericType.BYTE_BUFFER;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.TEXT;
    }

    @Override
    public @Nullable ByteBuffer encode(@Nullable ByteBuffer value, @NotNull ProtocolVersion protocolVersion) {
        return value;
    }

    @Override
    public @Nullable ByteBuffer decode(@Nullable ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        if (bytes == null) {
            return null;
        }
        return ByteBuffer.wrap(ByteBufferUtil.getArray(bytes.duplicate()));
    }

    @Override
    public @NotNull String format(@Nullable ByteBuffer value) {
        if (value == null || !value.hasArray()) {
            return StringUtils.EMPTY;
        }
        return new String(value.array(), StandardCharsets.UTF_8);
    }

    @Override
    public @Nullable ByteBuffer parse(@Nullable String value) {
        if (value == null) {
            return null;
        }
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

}
