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

import com.datastax.oss.driver.api.core.data.TupleValue;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.sql.Types;

/**
 * JDBC description of CQL tuple (corresponding Java type: {@link TupleValue}).
 * <p>CQL type description: a group of 2-3 fields.</p>
 */
public class JdbcTuple extends AbstractJdbcType<TupleValue> {

    /**
     * Gets a {@code JdbcTuple} instance.
     */
    public static final JdbcTuple INSTANCE = new JdbcTuple();

    JdbcTuple() {
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
    public String toString(@NonNull final TupleValue obj) {
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
    public Class<TupleValue> getType() {
        return TupleValue.class;
    }

    @Override
    public int getJdbcType() {
        return Types.OTHER;
    }

    @Override
    public TupleValue compose(final Object obj) {
        return (TupleValue) obj;
    }

    @Override
    public Object decompose(final TupleValue value) {
        return value;
    }

    @Override
    public int getScale(final TupleValue obj) {
        return DEFAULT_SCALE;
    }

    @Override
    public int getPrecision(final TupleValue obj) {
        return DEFAULT_PRECISION;
    }

}
