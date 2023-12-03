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

import java.sql.Types;

/**
 * Abstract class providing description about the JDBC equivalent of any CQL type representing a collection.
 */
public abstract class AbstractJdbcCollection<T> extends AbstractJdbcType<T> {

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public int getScale(final T obj) {
        return DEFAULT_SCALE;
    }

    @Override
    public int getPrecision(final T obj) {
        return DEFAULT_PRECISION;
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
    public String toString(final T obj) {
        return obj.toString();
    }

    @Override
    public boolean needsQuotes() {
        return false;
    }

    @Override
    public abstract Class<T> getType();

    @Override
    public int getJdbcType() {
        return Types.OTHER;
    }

    @Override
    public abstract T compose(Object obj);

    @Override
    public abstract Object decompose(T value);

}
