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
package com.ing.data.cassandra.jdbc;

import java.nio.ByteBuffer;
import java.sql.Types;


public class JdbcBytes extends AbstractJdbcType<ByteBuffer> {
    public static final JdbcBytes instance = new JdbcBytes();

    JdbcBytes() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(final ByteBuffer obj) {
        return -1;
    }

    public int getPrecision(final ByteBuffer obj) {
        return -1;
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return false;
    }

    public String toString(final ByteBuffer obj) {
        return "";
    }

    public boolean needsQuotes() {
        return true;
    }

    public Class<ByteBuffer> getType() {
        return ByteBuffer.class;
    }

    public int getJdbcType() {
        return Types.BINARY;
    }

    public ByteBuffer compose(final ByteBuffer bytes) {
        return bytes.duplicate();
    }

    public ByteBuffer decompose(final ByteBuffer value) {
        return value;
    }

    @Override
    public ByteBuffer compose(final Object obj) {
        return null;
    }
}
