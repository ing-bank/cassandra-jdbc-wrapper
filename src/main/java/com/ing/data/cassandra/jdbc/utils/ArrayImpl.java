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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.ing.data.cassandra.jdbc.types.DataTypeEnum;
import com.ing.data.cassandra.jdbc.types.TypesMap;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.CUSTOM;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.ARRAY_ITEM_CONVERSION_FAILED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.INVALID_NULL_TYPE_FOR_ARRAY;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.UNSUPPORTED_CQL_TYPE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNullElseGet;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.collections4.MapUtils.isNotEmpty;
import static org.apache.commons.lang3.ArrayUtils.subarray;

/**
 * Implementation of {@link Array} interface.
 */
public class ArrayImpl implements Array {

    private Object[] array;
    private DataTypeEnum cqlDataType;

    /**
     * Constructor of an empty array.
     */
    public ArrayImpl() {
        this.array = null;
        this.cqlDataType = CUSTOM;
    }

    /**
     * Constructor.
     *
     * @param list The list of object to wrap in a {@link Array} object.
     */
    public ArrayImpl(final List<?> list) {
        buildArray(list);
    }

    /**
     * Constructor with target type.
     *
     * @param list     The list of object to wrap in a {@link Array} object.
     * @param typeName The CQL type of the wrapped objects. A conversion to the appropriate type is done if possible.
     *                 TODO: document supported types and conversions
     * @throws SQLException if the JDBC type is not appropriate for the specified CQL type and the conversion is not
     * supported, or if the specified type is {@code null}.
     * @throws SQLFeatureNotSupportedException if the specified CQL type is not supported.
     */
    public ArrayImpl(final List<?> list, final String typeName) throws SQLException {
        if (typeName == null) {
            throw new SQLException(INVALID_NULL_TYPE_FOR_ARRAY);
        }
        final DataTypeEnum cqlDataTypeFromTypeName = DataTypeEnum.fromCqlTypeName(typeName);
        if (cqlDataTypeFromTypeName == null) {
            throw new SQLFeatureNotSupportedException(format(UNSUPPORTED_CQL_TYPE, typeName));
        }
        this.cqlDataType = cqlDataTypeFromTypeName;

        final List<Object> convertedList = new ArrayList<>();
        for (final Object item : list) {
            try {
                convertedList.add(convertObjectIfRequired(item, cqlDataTypeFromTypeName));
            } catch (final Exception e) {
                String itemClassName = "undefined";
                if (item != null) {
                    itemClassName = item.getClass().getName();
                }
                throw new SQLException(format(ARRAY_ITEM_CONVERSION_FAILED,
                    itemClassName, cqlDataType.asJavaClass(), cqlDataType.getName(), e.getMessage()));
            }
        }
        buildArray(convertedList);
    }

    @Override
    public String getBaseTypeName() {
        return this.cqlDataType.asLowercaseCql();
    }

    @Override
    public int getBaseType() {
        return TypesMap.getTypeForComparator(this.cqlDataType.cqlType).getJdbcType();
    }

    @Override
    public Object[] getArray() throws SQLException {
        return (Object[]) getArray(1, getSafeContent().length);
    }

    @Override
    public Object getArray(final Map<String, Class<?>> map) throws SQLException {
        return getArray(1, getSafeContent().length, map);
    }

    @Override
    public Object getArray(final long index, final int count) throws SQLException {
        return getArray(index, count, null);
    }

    @Override
    public Object getArray(final long index, final int count, final Map<String, Class<?>> map) throws SQLException {
        // Maps are currently not supported.
        if (isNotEmpty(map)) {
            throw new SQLFeatureNotSupportedException();
        }

        final int zeroBasedStartIndex = (int) index - 1;
        return subarray(getSafeContent(), zeroBasedStartIndex, zeroBasedStartIndex + count);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return getResultSet(1, getSafeContent().length);
    }

    @Override
    public ResultSet getResultSet(final Map<String, Class<?>> map) throws SQLException {
        return getResultSet(1, getSafeContent().length, map);
    }

    @Override
    public ResultSet getResultSet(final long index, final int count) throws SQLException {
        return getResultSet(index, count, null);
    }

    @Override
    public ResultSet getResultSet(final long index,
                                  final int count,
                                  final Map<String, Class<?>> map) throws SQLException {
        return buildResultSetFromArray((Object[]) this.getArray(index, count, map));
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
        return format("[%s]",
            Arrays.stream(getSafeContent())
                .map(item -> Objects.toString(item, nullValueAsString))
                .collect(joining(", "))
        );
    }

    /**
     * Gets a {@link List} instance corresponding to this array.
     *
     * @return The list instance corresponding to this {@link Array} instance, or {@code null} if the array content is
     * {@code null}.
     */
    public List<Object> toList() {
        if (this.array != null) {
            return Arrays.asList(getSafeContent());
        }
        return null;
    }

    private Object[] getSafeContent() {
        return requireNonNullElseGet(this.array, () -> new Object[0]);
    }

    private void buildArray(final List<?> list) {
        final Object[] builtArray = new Object[list.size()];
        for (int i = 0; i < list.size(); i++) {
            builtArray[i] = list.get(i);
        }
        this.array = builtArray;

        if (this.cqlDataType == null) {
            if (!list.isEmpty()) {
                this.cqlDataType = DataTypeEnum.fromJavaType(list.get(0).getClass());
            } else {
                this.cqlDataType = CUSTOM;
            }
        }
    }

    private ResultSet buildResultSetFromArray(final Object[] slicedArray) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
        // TODO: implement this method.
    }

    private static Object convertObjectIfRequired(
        final Object element, final DataTypeEnum type
    ) throws IllegalArgumentException, UnknownHostException, SecurityException, SQLFeatureNotSupportedException {
        if (element == null) {
            return null;
        }

        final String elementAsString = element.toString();
        return switch (type) {
            case ASCII, TEXT, VARCHAR -> elementAsString;
            case BIGINT, COUNTER -> Long.parseLong(elementAsString);
            // FIXME case BLOB, CUSTOM -> bytes(elementAsString);
            case BOOLEAN -> Boolean.parseBoolean(elementAsString);
            // FIXME case DATE -> Date.valueOf(elementAsString); only accept some specific types and convert to Date
            case DECIMAL -> new BigDecimal(elementAsString);
            case DOUBLE -> Double.valueOf(elementAsString);
            case DURATION -> CqlDuration.from(elementAsString);
            case FLOAT -> Float.valueOf(elementAsString);
            case INET -> InetAddress.getByName(elementAsString);
            case INT -> Integer.parseInt(elementAsString);
            case SMALLINT -> Short.valueOf(elementAsString);
            // FIXME case TIME -> Time.valueOf(elementAsString); only accept some specific types and convert to Time
            // FIXME case TIMESTAMP -> Timestamp.valueOf(elementAsString); only accept some  types and convert to Ts
            case TIMEUUID, UUID -> UUID.fromString(elementAsString);
            case TINYINT -> Byte.valueOf(elementAsString);
            case VARINT -> new BigInteger(elementAsString);
            // LIST, MAP, SET, UDT and VECTOR are currently not supported.
            default -> throw new SQLFeatureNotSupportedException("The specified CQL type is not supported.");
        };
    }

}
