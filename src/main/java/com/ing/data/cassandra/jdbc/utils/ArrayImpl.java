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

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.driver.internal.core.cql.DefaultRow;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.ing.data.cassandra.jdbc.CassandraResultSet;
import com.ing.data.cassandra.jdbc.ColumnDefinitions;
import com.ing.data.cassandra.jdbc.types.DataTypeEnum;
import com.ing.data.cassandra.jdbc.types.TypesMap;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static com.datastax.oss.driver.api.core.detach.AttachmentPoint.NONE;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.CUSTOM;
import static com.ing.data.cassandra.jdbc.utils.ByteBufferUtil.bytes;
import static com.ing.data.cassandra.jdbc.utils.ConversionsUtil.convertToSqlDate;
import static com.ing.data.cassandra.jdbc.utils.ConversionsUtil.convertToSqlTime;
import static com.ing.data.cassandra.jdbc.utils.ConversionsUtil.convertToSqlTimestamp;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.buildDriverResultSet;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.ARRAY_ITEM_CONVERSION_FAILED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.INVALID_NULL_TYPE_FOR_ARRAY;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.UNSUPPORTED_CQL_TYPE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNullElseGet;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.collections4.MapUtils.isNotEmpty;
import static org.apache.commons.lang3.ArrayUtils.subarray;
import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Implementation of {@link Array} interface.
 */
public class ArrayImpl implements Array {

    private Object[] array;
    private final DataTypeEnum cqlDataType;

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
     * @param list The list of objects to wrap in a {@link Array} object.
     * @throws SQLException if the type of specified of objects are not supported.
     */
    public ArrayImpl(final List<?> list) throws SQLException {
        this(list, determineCqlType(list));
    }

    /**
     * Constructor with target type.
     *
     * @param list     The list of object to wrap in a {@link Array} object.
     * @param typeName The CQL type of the wrapped objects. A conversion to the appropriate type is done if possible.
     *                 The supported CQL types and the corresponding Java types are listed below:
     *                 <table border="1">
     *                 <caption>Supported CQL data types and corresponding Java classes for JDBC Arrays</caption>
     *                 <tr><th>CQL data type</th><th>Supported Java class(es)</th></tr>
     *                 <tr><td>ASCII    </td><td>{@link Object} <sup>1</sup></td></tr>
     *                 <tr><td>BIGINT   </td><td>{@link Object} <sup>1, 2</sup></td></tr>
     *                 <tr><td>BLOB     </td>
     *                     <td>{@link ByteBuffer}, {@link String}, {@link Byte}, {@link Short}, {@link Integer},
     *                     {@link Long}, {@link Float}, {@link Double}, {@link Boolean}, {@link BigDecimal},
     *                     {@link BigInteger}, {@link CqlDuration}, {@link InetAddress}, {@link Date}, {@link Time},
     *                     {@link Timestamp}, or {@link UUID}</td></tr>
     *                 <tr><td>BOOLEAN  </td><td>{@link Object} <sup>1, 3</sup></td></tr>
     *                 <tr><td>COUNTER  </td><td>{@link Object} <sup>1, 2</sup></td></tr>
     *                 <tr><td>CUSTOM   </td>
     *                     <td>{@link ByteBuffer}, {@link String}, {@link Byte}, {@link Short}, {@link Integer},
     *                     {@link Long}, {@link Float}, {@link Double}, {@link Boolean}, {@link BigDecimal},
     *                     {@link BigInteger}, {@link CqlDuration}, {@link InetAddress}, {@link Date}, {@link Time},
     *                     {@link Timestamp}, or {@link UUID}</td></tr>
     *                 <tr><td>DATE     </td>
     *                     <td>{@link Date}, {@link LocalDate}, {@link java.util.Date}, {@link Instant}, or
     *                     {@link String}<sup>4</sup></td></tr>
     *                 <tr><td>DECIMAL  </td><td>{@link Object} <sup>1, 2</sup></td></tr>
     *                 <tr><td>DOUBLE   </td><td>{@link Object} <sup>1, 2</sup></td></tr>
     *                 <tr><td>DURATION </td>
     *                     <td>{@link CqlDuration}, {@link Duration}, or {@link String}<sup>5</sup></td></tr>
     *                 <tr><td>FLOAT    </td><td>{@link Object} <sup>1, 2</sup></td></tr>
     *                 <tr><td>INET     </td><td>{@link InetAddress} or {@link String}<sup>6</sup></td></tr>
     *                 <tr><td>INT      </td><td>{@link Object} <sup>1, 2</sup></td></tr>
     *                 <tr><td>SMALLINT </td><td>{@link Object} <sup>1, 2</sup></td></tr>
     *                 <tr><td>TEXT     </td><td>{@link Object} <sup>1</sup></td></tr>
     *                 <tr><td>TIME     </td>
     *                     <td>{@link Time}, {@link LocalTime}, {@link OffsetTime}, {@link Instant},
     *                     or {@link String}<sup>7</sup></td></tr>
     *                 <tr><td>TIMESTAMP</td>
     *                     <td>{@link Timestamp}, {@link LocalDateTime}, {@link OffsetDateTime}, {@link Calendar},
     *                     {@link Instant}, or {@link String}<sup>8</sup></td></tr>
     *                 <tr><td>TIMEUUID </td><td>{@link UUID} or {@link String}<sup>6</sup></td></tr>
     *                 <tr><td>TINYINT  </td><td>{@link Object} <sup>1, 2</sup></td></tr>
     *                 <tr><td>UUID     </td><td>{@link UUID} or {@link String}<sup>9</sup></td></tr>
     *                 <tr><td>VARCHAR  </td><td>{@link Object} <sup>1</sup></td></tr>
     *                 <tr><td>VARINT   </td><td>{@link Object} <sup>1, 2</sup></td></tr>
     *                 </table>
     *                 <p>
     *                     <sup>1</sup> When supported Java type is {@link Object}, the value is converted to
     *                     {@link String} value first, using the method {@code toString()}.
     *                 </p>
     *                 <p>
     *                     <sup>2</sup> Only valid numeric representations are allowed, otherwise an exception will be
     *                     thrown. Also, the represented numeric value must respect the boundaries of the target numeric
     *                     type.
     *                 </p>
     *                 <p>
     *                     <sup>3</sup> see {@link Boolean#parseBoolean(String)} specifications regarding the applied
     *                     parsing rules.
     *                 </p>
     *                 <p>
     *                     <sup>4</sup> see {@link Date#valueOf(String)} specifications regarding the applied parsing
     *                     rules.
     *                 </p>
     *                 <p>
     *                     <sup>5</sup> see {@link CqlDuration#from(String)} specifications regarding the applied
     *                     parsing rules.
     *                 </p>
     *                 <p>
     *                     <sup>6</sup> see {@link InetAddress#getByName(String)} specifications regarding the applied
     *                     parsing rules.
     *                 </p>
     *                 <p>
     *                     <sup>7</sup> see {@link Time#valueOf(String)} specifications regarding the applied parsing
     *                     rules.
     *                 </p>
     *                 <p>
     *                     <sup>8</sup> see {@link Timestamp#valueOf(String)} specifications regarding the applied
     *                     parsing rules.
     *                 </p>
     *                 <p>
     *                     <sup>9</sup> see {@link UUID#fromString(String)} specifications regarding the applied
     *                     parsing rules.
     *                 </p>
     *                 <p>
     *                     The following CQL types are currently not supported: {@code LIST}, {@code MAP}, {@code SET},
     *                     {@code TUPLE}, {@code UDT} and {@code VECTOR}.
     *                 </p>
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
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
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
        return buildResultSetFromArray((Object[]) this.getArray(index, count, map), index);
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
    }

    private ResultSet buildResultSetFromArray(final Object[] slicedArray, final long startIndex) throws SQLException {
        // Build columns definitions: first column is the index of the element in the array, the second one is the
        // element value.
        final List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(new DefaultColumnDefinition(
            new ColumnDefinitions.Definition(EMPTY, EMPTY, "index", DataTypes.INT).toColumnSpec(0), NONE)
        );
        // Only CQL primitive types are currently supported (i.e. LIST, MAP, SET, TUPLE, UDT and VECTOR are
        // excluded). Additionally, in this implementation, we consider CUSTOM type as BLOB.
        int cqlTypeProtocolId = this.cqlDataType.getProtocolId();
        if (cqlTypeProtocolId == ProtocolConstants.DataType.CUSTOM) {
            cqlTypeProtocolId = ProtocolConstants.DataType.BLOB;
        }
        final DataType valueColumnType = new PrimitiveType(cqlTypeProtocolId);
        columnDefinitions.add(new DefaultColumnDefinition(
            new ColumnDefinitions.Definition(EMPTY, EMPTY, "value", valueColumnType).toColumnSpec(1), NONE)
        );
        final com.datastax.oss.driver.api.core.cql.ColumnDefinitions rsColumns =
            DefaultColumnDefinitions.valueOf(columnDefinitions);

        // Populate rows.
        final List<Row> rsRows = new ArrayList<>();
        for (int i = 0; i < slicedArray.length; i++) {
            final var index = i + (int) startIndex;
            final var value = slicedArray[i];
            rsRows.add(new DefaultRow(rsColumns, List.of(bytes(index), bytes(value))));
        }

        return CassandraResultSet.buildFrom(buildDriverResultSet(rsColumns, rsRows));
    }

    private static String determineCqlType(final List<?> list) {
        if (!list.isEmpty()) {
            final boolean hasMixedTypes = list.stream()
                .map(Object::getClass)
                .distinct()
                .count() > 1;
            if (hasMixedTypes) {
                return CUSTOM.asLowercaseCql();
            }
            return DataTypeEnum.fromJavaType(list.get(0).getClass()).asLowercaseCql();
        }
        return CUSTOM.asLowercaseCql();
    }

    private static Object convertObjectIfRequired(
        final Object element, final DataTypeEnum type
    ) throws IllegalArgumentException, UnknownHostException, SecurityException, SQLException {
        if (element == null) {
            return null;
        }

        final String elementAsString = element.toString();
        return switch (type) {
            case ASCII, TEXT, VARCHAR -> elementAsString;
            case BIGINT, COUNTER -> Long.parseLong(elementAsString);
            case BLOB, CUSTOM -> bytes(element);
            case BOOLEAN -> Boolean.parseBoolean(elementAsString);
            case DATE -> convertToSqlDate(element);
            case DECIMAL -> new BigDecimal(elementAsString);
            case DOUBLE -> Double.valueOf(elementAsString);
            case DURATION -> CqlDuration.from(elementAsString);
            case FLOAT -> Float.valueOf(elementAsString);
            case INET -> InetAddress.getByName(elementAsString);
            case INT -> Integer.parseInt(elementAsString);
            case SMALLINT -> Short.valueOf(elementAsString);
            case TIME -> convertToSqlTime(element);
            case TIMESTAMP -> convertToSqlTimestamp(element);
            case TIMEUUID, UUID -> UUID.fromString(elementAsString);
            case TINYINT -> Byte.valueOf(elementAsString);
            case VARINT -> new BigInteger(elementAsString);
            // LIST, MAP, SET, TUPLE, UDT and VECTOR are currently not supported.
            default -> throw new SQLFeatureNotSupportedException("The specified CQL type is not supported.");
        };
    }

}
