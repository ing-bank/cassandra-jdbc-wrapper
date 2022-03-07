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

import com.datastax.oss.driver.api.core.data.UdtValue;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.sql.Types;

/**
 * JDBC description of a CQL user-defined type (corresponding Java type: {@link UdtValue}).
 */
public class JdbcUdt extends AbstractJdbcType<UdtValue> {

    /**
     * Gets a {@code JdbcUdt} instance.
     */
    public static final JdbcUdt INSTANCE = new JdbcUdt();

    JdbcUdt() {
    }

    @Override
    public boolean isCaseSensitive() {
        return true;
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
    public String toString(@NonNull final UdtValue obj) {
        return getString(obj);
    }

    @Override
    public boolean needsQuotes() {
        return true;
    }

    public String getString(final Object obj) {
        if (obj != null) {
            return obj.toString();
        } else {
            return null;
        }
    }

    @Override
    public Class<UdtValue> getType() {
        return UdtValue.class;
    }

    @Override
    public int getJdbcType() {
        return Types.OTHER;
    }

    @Override
    public UdtValue compose(final Object obj) {
        return (UdtValue) obj;
    }

    @Override
    public Object decompose(final UdtValue value) {
        return value;
    }

    @Override
    public int getScale(final UdtValue obj) {
        return DEFAULT_SCALE;
    }

    @Override
    public int getPrecision(final UdtValue obj) {
        return DEFAULT_PRECISION;
    }

}
