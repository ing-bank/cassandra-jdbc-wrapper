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
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public class NotInstantiableValidTestCodec implements TypeCodec<String> {

    private NotInstantiableValidTestCodec() {
        // Make the no-args constructor private to prevent the instantiation of the codec.
    }

    @Override
    public @NotNull GenericType<String> getJavaType() {
        return null;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return null;
    }

    @Override
    public @Nullable ByteBuffer encode(@Nullable String value, @NotNull ProtocolVersion protocolVersion) {
        return null;
    }

    @Override
    public @Nullable String decode(@Nullable ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        return "";
    }

    @Override
    public @NotNull String format(@Nullable String value) {
        return "";
    }

    @Override
    public @Nullable String parse(@Nullable String value) {
        return "";
    }

}
