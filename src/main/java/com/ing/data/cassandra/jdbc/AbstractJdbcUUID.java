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

import java.sql.Types;
import java.util.UUID;

/**
 * Abstract class providing description about the JDBC equivalent of any CQL type based on the Java type {@link UUID}.
 */
public abstract class AbstractJdbcUUID extends AbstractJdbcType<UUID> {

    // By default, UUID format always contains 32 hexadecimal characters (base-16 digits) and 4 hyphens.
    private static final int DEFAULT_UUID_PRECISION = 36;

    @Override
    public String toString(@NonNull final UUID obj) {
        return obj.toString();
    }

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public int getScale(final UUID obj) {
        return DEFAULT_SCALE;
    }

    @Override
    public int getPrecision(final UUID obj) {
        if (obj != null) {
            return toString(obj).length();
        }
        return DEFAULT_UUID_PRECISION;
    }

    @Override
    public boolean isCurrency() {
        return false;
    }

    @Override
    public boolean isSigned() {
        return false;
    }

    @Override
    public boolean needsQuotes() {
        return false;
    }

    @Override
    public Class<UUID> getType() {
        return UUID.class;
    }

    @Override
    public int getJdbcType() {
        return Types.OTHER;
    }

}
