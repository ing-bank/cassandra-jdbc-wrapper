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

package com.ing.data.cassandra.jdbc.types;


import javax.annotation.Nonnull;
import java.util.UUID;

/**
 * JDBC description of {@code TIMEUUID} CQL type (corresponding Java type: {@link UUID}).
 * <p>CQL type description: version 1 UUID only.</p>
 */
public class JdbcTimeUUID extends AbstractJdbcUUID {

    /**
     * Gets a {@code JdbcTimeUUID} instance.
     */
    public static final JdbcTimeUUID INSTANCE = new JdbcTimeUUID();

    JdbcTimeUUID() {
    }

    @Override
    public UUID compose(@Nonnull final Object obj) {
        return UUID.fromString(obj.toString());
    }

    @Override
    public Object decompose(@Nonnull final UUID value) {
        return value.toString();
    }

}
