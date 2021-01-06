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

import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * JDBC description of {@code UUID} CQL type (corresponding Java type: {@link UUID}).
 * <p>CQL type description: a UUID in standard UUID format.</p>
 */
public class JdbcUUID extends AbstractJdbcUUID {

    /**
     * Gets a {@code JdbcUUID} instance.
     */
    public static final JdbcUUID instance = new JdbcUUID();

    JdbcUUID() {
    }

    public UUID compose(ByteBuffer bytes) {
        bytes = bytes.slice();
        if (bytes.remaining() < 16) {
            return new UUID(0, 0);
        }
        return new UUID(bytes.getLong(), bytes.getLong());
    }

    public String getString(final ByteBuffer bytes) {
        if (bytes.remaining() == 0) {
            return StringUtils.EMPTY;
        }
        if (bytes.remaining() != 16) {
            throw new MarshalException("UUIDs must be exactly 16 bytes, but found: " + bytes.remaining());
        }
        return compose(bytes).toString();
    }

    public UUID compose(@NonNull final Object obj) {
        return UUID.fromString(obj.toString());
    }

    public Object decompose(final UUID obj) {
        return obj;
    }

}
