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

package com.ing.data.cassandra.jdbc.utils;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.ARRAY_WAS_FREED;

/**
 * Implementation of {@link Array} interface.
 */
public class ArrayImpl implements Array {

    private Object[] array;

    /**
     * Constructor.
     *
     * @param list The list of object to wrap in a {@link Array} object.
     */
    public ArrayImpl(final List<?> list) {
        final Object[] array = new Object[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        this.array = array;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getBaseType() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object[] getArray() throws SQLException {
        if (this.array == null) {
            throw new SQLException(ARRAY_WAS_FREED);
        }
        return this.array;
    }

    @Override
    public Object getArray(final Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getArray(final long index, final int count) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getArray(final long index, final int count, final Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(final Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(final long index, final int count) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(final long index, final int count, final Map<String, Class<?>> map)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void free() {
        if (this.array != null) {
            this.array = null;
        }
    }

    @Override
    public String toString() {
        final String nullValueAsString = "null";
        if (this.array != null) {
            return String.format("[%s]",
                Arrays.stream(this.array)
                    .map(item -> Objects.toString(item, nullValueAsString))
                    .collect(Collectors.joining(", "))
            );
        }
        return nullValueAsString;
    }

    /**
     * Gets a {@link List} instance corresponding to this array.
     *
     * @return The list instance corresponding to this {@link Array} instance.
     */
    public List<Object> toList() {
        if (this.array != null) {
            return Arrays.asList(this.array);
        }
        return null;
    }

}
