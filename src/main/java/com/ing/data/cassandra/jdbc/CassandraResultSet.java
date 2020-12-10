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

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import static com.ing.data.cassandra.jdbc.Utils.BAD_FETCH_DIR;
import static com.ing.data.cassandra.jdbc.Utils.BAD_FETCH_SIZE;
import static com.ing.data.cassandra.jdbc.Utils.FORWARD_ONLY;
import static com.ing.data.cassandra.jdbc.Utils.MUST_BE_POSITIVE;
import static com.ing.data.cassandra.jdbc.Utils.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.Utils.NO_INTERFACE;
import static com.ing.data.cassandra.jdbc.Utils.VALID_LABELS;
import static com.ing.data.cassandra.jdbc.Utils.WAS_CLOSED_RSLT;

/**
 * A Cassandra result set.
 * <p>
 * The Supported Data types in CQL are as follows:
 * </p>
 * <table>
 * <tr>
 * <th>type</th>
 * <th>java type</th>
 * <th>description</th>
 * </tr>
 * <tr>
 * <td>ascii</td>
 * <td>String</td>
 * <td>ASCII character string</td>
 * </tr>
 * <tr>
 * <td>bigint</td>
 * <td>Long</td>
 * <td>64-bit signed long</td>
 * </tr>
 * <tr>
 * <td>blob</td>
 * <td>ByteBuffer</td>
 * <td>Arbitrary bytes (no validation)</td>
 * </tr>
 * <tr>
 * <td>boolean</td>
 * <td>Boolean</td>
 * <td>true or false</td>
 * </tr>
 * <tr>
 * <td>counter</td>
 * <td>Long</td>
 * <td>Counter column (64-bit long)</td>
 * </tr>
 * <tr>
 * <td>decimal</td>
 * <td>BigDecimal</td>
 * <td>Variable-precision decimal</td>
 * </tr>
 * <tr>
 * <td>double</td>
 * <td>Double</td>
 * <td>64-bit IEEE-754 floating point</td>
 * </tr>
 * <tr>
 * <td>float</td>
 * <td>Float</td>
 * <td>32-bit IEEE-754 floating point</td>
 * </tr>
 * <tr>
 * <td>int</td>
 * <td>Integer</td>
 * <td>32-bit signed int</td>
 * </tr>
 * <tr>
 * <td>text</td>
 * <td>String</td>
 * <td>UTF8 encoded string</td>
 * </tr>
 * <tr>
 * <td>timestamp</td>
 * <td>Date</td>
 * <td>A timestamp</td>
 * </tr>
 * <tr>
 * <td>uuid</td>
 * <td>UUID</td>
 * <td>Type 1 or type 4 UUID</td>
 * </tr>
 * <tr>
 * <td>varchar</td>
 * <td>String</td>
 * <td>UTF8 encoded string</td>
 * </tr>
 * <tr>
 * <td>varint</td>
 * <td>BigInteger</td>
 * <td>Arbitrary-precision integer</td>
 * </tr>
 * </table>
 */
class CassandraResultSet extends AbstractResultSet implements CassandraResultSetExtras {
    private static final Logger log = LoggerFactory.getLogger(CassandraResultSet.class);

    public static final int DEFAULT_TYPE = TYPE_FORWARD_ONLY;
    public static final int DEFAULT_CONCURRENCY = CONCUR_READ_ONLY;
    public static final int DEFAULT_HOLDABILITY = HOLD_CURSORS_OVER_COMMIT;
    private Row currentRow;

    /**
     * The rows iterator.
     */
    private Iterator<Row> rowsIterator;

    int rowNumber = 0;
    // the current row key when iterating through results.
    private final byte[] curRowKey = null;

    private final CResultSetMetaData meta;

    private final CassandraStatement statement;

    private int resultSetType;

    private int fetchDirection;

    private int fetchSize;

    private boolean wasNull;

    private ResultSet driverResultSet;

    /**
     * No argument constructor.
     */
    CassandraResultSet() {
        statement = null;
        meta = new CResultSetMetaData();
    }

    /**
     * Instantiates a new Cassandra result set from a {@link ResultSet}.
     */
    CassandraResultSet(final CassandraStatement statement, final ResultSet resultSet) throws SQLException {
        this.statement = statement;
        this.resultSetType = statement.getResultSetType();
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();
        this.driverResultSet = resultSet;

        rowsIterator = resultSet.iterator();
        if (hasMoreRows()) {
            populateColumns();
        }

        meta = new CResultSetMetaData();
    }

    /**
     * Instantiates a new Cassandra result set from a list of {@link ResultSet}.
     */
    CassandraResultSet(final CassandraStatement statement, final ArrayList<ResultSet> resultSets) throws SQLException {
        this.statement = statement;
        this.resultSetType = statement.getResultSetType();
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();

        // We have several result sets, but we will use only the first one for metadata needs.
        this.driverResultSet = resultSets.get(0);

        // Now we concatenate iterators of the different result sets into a single one.
        rowsIterator = driverResultSet.iterator();
        for (int i = 1; i < resultSets.size(); i++) {
            // this leads to StackOverflow exception when there are too many result sets.
            rowsIterator = Iterators.concat(rowsIterator, resultSets.get(i).iterator());
        }

        // Initialize the column values from the first row.
        if (hasMoreRows()) {
            populateColumns();
        }

        meta = new CResultSetMetaData();
    }

    private boolean hasMoreRows() {
        return (rowsIterator != null && (rowsIterator.hasNext() || (rowNumber == 0 && currentRow != null)));
    }

    private void populateColumns() {
        currentRow = rowsIterator.next();
    }

    @Override
    public boolean absolute(final int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void afterLast() throws SQLException {
        if (resultSetType == TYPE_FORWARD_ONLY) {
            throw new SQLNonTransientException(FORWARD_ONLY);
        }
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (resultSetType == TYPE_FORWARD_ONLY) {
            throw new SQLNonTransientException(FORWARD_ONLY);
        }
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    private void checkIndex(final int index) throws SQLException {
        if (currentRow != null) {
            wasNull = currentRow.isNull(index - 1);
            if (index < 1 || index > currentRow.getColumnDefinitions().size()) {
                throw new SQLSyntaxErrorException(String.format(MUST_BE_POSITIVE, index) + StringUtils.SPACE
                    + currentRow.getColumnDefinitions().size());
            }
        } else if (driverResultSet != null) {
            if (index < 1 || index > driverResultSet.getColumnDefinitions().size()) {
                throw new SQLSyntaxErrorException(String.format(MUST_BE_POSITIVE, index) + StringUtils.SPACE
                    + driverResultSet.getColumnDefinitions().size());
            }
        }

    }

    private void checkName(final String name) throws SQLException {
        if (currentRow != null) {
            wasNull = currentRow.isNull(name);
            if (!currentRow.getColumnDefinitions().contains(name)) {
                throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
            }
        } else if (driverResultSet != null) {
            if (!driverResultSet.getColumnDefinitions().contains(name)) {
                throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
            }
        }
    }

    private void checkNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLRecoverableException(WAS_CLOSED_RSLT);
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    @Override
    public void close() throws SQLException {
        if (!isClosed()) {
            this.statement.close();
        }
    }

    @Override
    public int findColumn(final String name) throws SQLException {
        checkNotClosed();
        checkName(name);
        return currentRow.getColumnDefinitions().firstIndexOf(name);
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public BigDecimal getBigDecimal(final int index) throws SQLException {
        checkIndex(index);
        return currentRow.getBigDecimal(index - 1);
    }

    /**
     * @deprecated
     */
    @Override
    @Deprecated
    public BigDecimal getBigDecimal(final int index, final int scale) throws SQLException {
        checkIndex(index);
        return currentRow.getBigDecimal(index - 1).setScale(scale);
    }

    @Override
    public BigDecimal getBigDecimal(final String name) throws SQLException {
        checkName(name);
        return currentRow.getBigDecimal(name);
    }

    /**
     * @deprecated
     */
    @Override
    @Deprecated
    public BigDecimal getBigDecimal(final String name, final int scale) throws SQLException {
        checkName(name);
        return currentRow.getBigDecimal(name).setScale(scale);
    }

    @Override
    public BigInteger getBigInteger(final int index) throws SQLException {
        checkIndex(index);
        return currentRow.getBigInteger(index - 1);
    }

    @Override
    public BigInteger getBigInteger(final String name) throws SQLException {
        checkName(name);
        return currentRow.getBigInteger(name);
    }

    @Override
    public boolean getBoolean(final int index) throws SQLException {
        checkIndex(index);
        return currentRow.getBoolean(index - 1);
    }

    @Override
    public boolean getBoolean(final String name) throws SQLException {
        checkName(name);
        return currentRow.getBoolean(name);
    }

    @Override
    public byte getByte(final int index) throws SQLException {
        checkIndex(index);
        return currentRow.getByte(index - 1);
    }

    @Override
    public byte getByte(final String name) throws SQLException {
        checkName(name);
        return currentRow.getByte(name);
    }

    @Override
    public byte[] getBytes(final int index) {
        final ByteBuffer byteBuffer = currentRow.getByteBuffer(index - 1);
        if (byteBuffer != null) {
            return byteBuffer.array();
        }
        return new byte[0];
    }

    @Override
    public byte[] getBytes(final String name) {
        final ByteBuffer byteBuffer = currentRow.getByteBuffer(name);
        if (byteBuffer != null) {
            return byteBuffer.array();
        }
        return new byte[0];
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkNotClosed();
        return statement.getResultSetConcurrency();
    }

    public LocalDate getLocalDate(final int index) throws SQLException {
        checkIndex(index);
        return currentRow.getLocalDate(index - 1);
    }

    @Override
    public Date getDate(final int index) throws SQLException {
        checkIndex(index);
        final LocalDate localDate = currentRow.getLocalDate(index - 1);
        if (localDate == null) {
            return null;
        } else {
            return java.sql.Date.valueOf(localDate);
        }
    }

    @Override
    public Date getDate(final int index, final Calendar calendar) throws SQLException {
        checkIndex(index);
        // silently ignore the Calendar argument; it's a hint we do not need
        return getDate(index - 1);
    }

    @Override
    public Date getDate(final String name) throws SQLException {
        checkName(name);
        final LocalDate localDate = currentRow.getLocalDate(name);
        if (localDate == null) {
            return null;
        } else {
            return java.sql.Date.valueOf(localDate);
        }
    }

    @Override
    public Date getDate(final String name, final Calendar calendar) throws SQLException {
        checkName(name);
        // silently ignore the Calendar argument; it's a hint we do not need
        return getDate(name);
    }

    @Override
    public double getDouble(final int index) throws SQLException {
        checkIndex(index);
        if ("float".equals(currentRow.getColumnDefinitions().get(index - 1).getType().asCql(false, false))) {
            return currentRow.getFloat(index - 1);
        }
        return currentRow.getDouble(index - 1);
    }

    @Override
    public double getDouble(final String name) throws SQLException {
        checkName(name);
        if ("float".equals(currentRow.getColumnDefinitions().get(name).getType().asCql(false, false))) {
            return currentRow.getFloat(name);
        }
        return currentRow.getDouble(name);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkNotClosed();
        return fetchDirection;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkNotClosed();
        return fetchSize;
    }

    @Override
    public float getFloat(final int index) throws SQLException {
        checkIndex(index);
        return currentRow.getFloat(index - 1);
    }

    @Override
    public float getFloat(final String name) throws SQLException {
        checkName(name);
        return currentRow.getFloat(name);
    }

    @Override
    public int getHoldability() throws SQLException {
        checkNotClosed();
        return statement.getResultSetHoldability();
    }

    @Override
    public int getInt(final int index) throws SQLException {
        checkIndex(index);
        return currentRow.getInt(index - 1);
    }

    @Override
    public int getInt(final String name) throws SQLException {
        checkName(name);
        return currentRow.getInt(name);
    }

    @Override
    public byte[] getKey() {
        return curRowKey;
    }

    @Override
    public List<?> getList(final int index) throws SQLException {
        checkIndex(index);
        if (DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().get(index - 1).getType().asCql(false, false))
            .isCollection()) {
            try {
                final ListType listType = (ListType) currentRow.getColumnDefinitions().get(index - 1).getType();
                return Lists.newArrayList(currentRow.getList(index - 1,
                    Class.forName(DataTypeEnum.fromCqlTypeName(listType.getElementType().asCql(false, false))
                        .asJavaClass().getCanonicalName())));
            } catch (final ClassNotFoundException e) {
                log.warn("Error while executing getList()", e);
            }
        }
        return currentRow.getList(index - 1, String.class);
    }

    @Override
    public List<?> getList(final String name) throws SQLException {
        checkName(name);
        if (DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().get(name).getType().asCql(false, false))
            .isCollection()) {
            try {
                final ListType listType = (ListType) currentRow.getColumnDefinitions().get(name).getType();
                return Lists.newArrayList(currentRow.getList(name,
                    Class.forName(DataTypeEnum.fromCqlTypeName(listType.getElementType().asCql(false, false))
                        .asJavaClass().getCanonicalName())));
            } catch (final ClassNotFoundException e) {
                log.warn("Error while executing getList()", e);
            }
        }
        return currentRow.getList(name, String.class);
    }

    @Override
    public long getLong(final int index) throws SQLException {
        checkIndex(index);
        if ("int".equals(currentRow.getColumnDefinitions().get(index - 1).getType().asCql(false, false))) {
            return currentRow.getInt(index - 1);
        } else if ("varint".equals(currentRow.getColumnDefinitions().get(index - 1).getType().asCql(false, false))) {
            final BigInteger bigintValue = currentRow.getBigInteger(index - 1);
            if (bigintValue != null) {
                return bigintValue.longValue();
            } else {
                return Long.MIN_VALUE;
            }
        } else {
            return currentRow.getLong(index - 1);
        }
    }

    @Override
    public long getLong(final String name) throws SQLException {
        checkName(name);
        if ("int".equals(currentRow.getColumnDefinitions().get(name).getType().asCql(false, false))) {
            return currentRow.getInt(name);
        } else if ("varint".equals(currentRow.getColumnDefinitions().get(name).getType().asCql(false, false))) {
            final BigInteger bigintValue = currentRow.getBigInteger(name);
            if (bigintValue != null) {
                return bigintValue.longValue();
            } else {
                return Long.MIN_VALUE;
            }
        } else {
            return currentRow.getLong(name);
        }
    }

    @Override
    public Map<?, ?> getMap(final int index) throws SQLException {
        checkIndex(index);
        wasNull = currentRow.isNull(index - 1);

        final DefaultMapType mapType = (DefaultMapType) currentRow.getColumnDefinitions().get(index).getType();
        final Class<?> keysClass = DataTypeEnum.fromCqlTypeName(mapType.getKeyType().asCql(false, false)).javaType;
        final Class<?> valuesClass = DataTypeEnum.fromCqlTypeName(mapType.getValueType().asCql(false, false)).javaType;

        return currentRow.getMap(index - 1, keysClass, valuesClass);
    }

    @Override
    public Map<?, ?> getMap(final String name) throws SQLException {
        checkName(name);

        final DefaultMapType mapType = (DefaultMapType) currentRow.getColumnDefinitions().get(name).getType();
        final Class<?> keysClass = DataTypeEnum.fromCqlTypeName(mapType.getKeyType().asCql(false, false)).javaType;
        final Class<?> valuesClass = DataTypeEnum.fromCqlTypeName(mapType.getValueType().asCql(false, false)).javaType;

        return currentRow.getMap(name, keysClass, valuesClass);
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return meta;
    }

    @Override
    @SuppressWarnings({"cast", "boxing"})
    public Object getObject(final int index) throws SQLException {
        checkIndex(index);
        final DataType datatype;

        if ("udt".equals(currentRow.getColumnDefinitions().get(index - 1).getType().asCql(false, false))) {
            return currentRow.getUdtValue(index - 1);
        }

        if ("tuple".equals(currentRow.getColumnDefinitions().get(index - 1).getType().asCql(false, false))) {
            return currentRow.getTupleValue(index - 1);
        }

        if (DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().get(index - 1).getType().asCql(false, false))
            .isCollection()) {
            if (currentRow.getColumnDefinitions().get(index - 1).getType().asCql(false, false).startsWith("set")) {
                final SetType setType = (SetType) currentRow.getColumnDefinitions().get(index - 1).getType();
                datatype = setType.getElementType();

                if (datatype instanceof UserDefinedType) {
                    return Sets.newLinkedHashSet(currentRow.getSet(index - 1, TypesMap.getTypeForComparator("udt")
                        .getType()));
                } else if (datatype instanceof TupleType) {
                    return Sets.newLinkedHashSet(currentRow.getSet(index - 1, TypesMap.getTypeForComparator("tuple")
                        .getType()));
                } else {
                    return Sets.newLinkedHashSet(currentRow.getSet(index - 1, TypesMap.getTypeForComparator(
                        datatype.asCql(false, false)).getType()));
                }
            }

            if (currentRow.getColumnDefinitions().get(index - 1).getType().asCql(false, false).startsWith("list")) {
                final ListType setType = (ListType) currentRow.getColumnDefinitions().get(index - 1).getType();
                datatype = setType.getElementType();
                if (datatype instanceof TupleType) {
                    return Lists.newArrayList(currentRow.getList(index - 1, TypesMap.getTypeForComparator("tuple")
                        .getType()));
                } else {
                    return Lists.newArrayList(currentRow.getList(index - 1, TypesMap.getTypeForComparator(
                        datatype.asCql(false, false)).getType()));
                }
            }

            if (currentRow.getColumnDefinitions().get(index - 1).getType().asCql(false, false).startsWith("map")) {
                final MapType mapType = (MapType) currentRow.getColumnDefinitions().get(index - 1).getType();
                final DataType keyDatatype = mapType.getKeyType();
                final DataType valueDatatype = mapType.getValueType();

                Class<?> keyType = TypesMap.getTypeForComparator(keyDatatype.asCql(false, false)).getType();
                if (keyDatatype instanceof UserDefinedType) {
                    keyType = TypesMap.getTypeForComparator("udt").getType();
                } else if (keyDatatype instanceof TupleType) {
                    keyType = TypesMap.getTypeForComparator("tuple").getType();
                }

                Class<?> valueType = TypesMap.getTypeForComparator(valueDatatype.asCql(false, false)).getType();
                if (valueDatatype instanceof UserDefinedType) {
                    valueType = TypesMap.getTypeForComparator("udt").getType();
                } else if (valueDatatype instanceof TupleType) {
                    valueType = TypesMap.getTypeForComparator("tuple").getType();
                }

                return Maps.newHashMap(currentRow.getMap(index - 1, keyType, valueType));
            }
        } else {
            final String typeName = currentRow.getColumnDefinitions().get(index - 1).getType().asCql(false, false);
            if ("varchar".equals(typeName)) {
                return currentRow.getString(index - 1);
            } else if ("ascii".equals(typeName)) {
                return currentRow.getString(index - 1);
            } else if ("integer".equals(typeName)) {
                return currentRow.getInt(index - 1);
            } else if ("bigint".equals(typeName)) {
                return currentRow.getLong(index - 1);
            } else if ("blob".equals(typeName)) {
                return currentRow.getByteBuffer(index - 1);
            } else if ("boolean".equals(typeName)) {
                return currentRow.getBoolean(index - 1);
            } else if ("counter".equals(typeName)) {
                return currentRow.getLong(index - 1);
            } else if ("decimal".equals(typeName)) {
                return currentRow.getBigDecimal(index - 1);
            } else if ("double".equals(typeName)) {
                return currentRow.getDouble(index - 1);
            } else if ("float".equals(typeName)) {
                return currentRow.getFloat(index - 1);
            } else if ("inet".equals(typeName)) {
                return currentRow.getInetAddress(index - 1);
            } else if ("int".equals(typeName)) {
                return currentRow.getInt(index - 1);
            } else if ("text".equals(typeName)) {
                return currentRow.getString(index - 1);
            } else if ("timestamp".equals(typeName)) {
                final Instant instant = currentRow.getInstant(index - 1);
                if (instant != null) {
                    return Timestamp.from(instant);
                } else {
                    return null;
                }
            } else if ("uuid".equals(typeName)) {
                return currentRow.getUuid(index - 1);
            } else if ("timeuuid".equals(typeName)) {
                return currentRow.getUuid(index - 1);
            } else if ("varint".equals(typeName)) {
                return currentRow.getInt(index - 1);
            }
        }

        return null;
    }

    @Override
    @SuppressWarnings({"cast", "boxing"})
    public Object getObject(final String name) throws SQLException {
        checkName(name);
        final DataType datatype;

        if ("udt".equals(currentRow.getColumnDefinitions().get(name).getType().asCql(false, false))) {
            return currentRow.getUdtValue(name);
        }

        if ("tuple".equals(currentRow.getColumnDefinitions().get(name).getType().asCql(false, false))) {
            return currentRow.getTupleValue(name);
        }

        if (DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().get(name).getType().asCql(false, false))
            .isCollection()) {
            if (currentRow.getColumnDefinitions().get(name).getType().asCql(false, false).startsWith("set")) {
                final SetType setType = (SetType) currentRow.getColumnDefinitions().get(name).getType();
                datatype = setType.getElementType();

                if (datatype instanceof UserDefinedType) {
                    return Sets.newLinkedHashSet(currentRow.getSet(name, TypesMap.getTypeForComparator("udt")
                        .getType()));
                } else if (datatype instanceof TupleType) {
                    return Sets.newLinkedHashSet(currentRow.getSet(name, TypesMap.getTypeForComparator("tuple")
                        .getType()));
                } else {
                    return Sets.newLinkedHashSet(currentRow.getSet(name, TypesMap.getTypeForComparator(
                        datatype.asCql(false, false)).getType()));
                }
            }

            if (currentRow.getColumnDefinitions().get(name).getType().asCql(false, false).startsWith("list")) {
                final ListType setType = (ListType) currentRow.getColumnDefinitions().get(name).getType();
                datatype = setType.getElementType();
                if (datatype instanceof TupleType) {
                    return Lists.newArrayList(currentRow.getList(name, TypesMap.getTypeForComparator("tuple")
                        .getType()));
                } else {
                    return Lists.newArrayList(currentRow.getList(name, TypesMap.getTypeForComparator(
                        datatype.asCql(false, false)).getType()));
                }
            }

            if (currentRow.getColumnDefinitions().get(name).getType().asCql(false, false).startsWith("map")) {
                final MapType mapType = (MapType) currentRow.getColumnDefinitions().get(name).getType();
                final DataType keyDatatype = mapType.getKeyType();
                final DataType valueDatatype = mapType.getValueType();

                Class<?> keyType = TypesMap.getTypeForComparator(keyDatatype.asCql(false, false)).getType();
                if (keyDatatype instanceof UserDefinedType) {
                    keyType = TypesMap.getTypeForComparator("udt").getType();
                } else if (keyDatatype instanceof TupleType) {
                    keyType = TypesMap.getTypeForComparator("tuple").getType();
                }

                Class<?> valueType = TypesMap.getTypeForComparator(valueDatatype.asCql(false, false)).getType();
                if (valueDatatype instanceof UserDefinedType) {
                    valueType = TypesMap.getTypeForComparator("udt").getType();
                } else if (valueDatatype instanceof TupleType) {
                    valueType = TypesMap.getTypeForComparator("tuple").getType();
                }

                return Maps.newHashMap(currentRow.getMap(name, keyType, valueType));
            }
        } else {
            final String typeName = currentRow.getColumnDefinitions().get(name).getType().asCql(false, false);

            if ("varchar".equals(typeName)) {
                return currentRow.getString(name);
            } else if ("ascii".equals(typeName)) {
                return currentRow.getString(name);
            } else if ("integer".equals(typeName)) {
                return currentRow.getInt(name);
            } else if ("bigint".equals(typeName)) {
                return currentRow.getLong(name);
            } else if ("blob".equals(typeName)) {
                return currentRow.getByteBuffer(name);
            } else if ("boolean".equals(typeName)) {
                return currentRow.getBoolean(name);
            } else if ("counter".equals(typeName)) {
                return currentRow.getLong(name);
            } else if ("decimal".equals(typeName)) {
                return currentRow.getBigDecimal(name);
            } else if ("double".equals(typeName)) {
                return currentRow.getDouble(name);
            } else if ("float".equals(typeName)) {
                return currentRow.getFloat(name);
            } else if ("inet".equals(typeName)) {
                return currentRow.getInetAddress(name);
            } else if ("int".equals(typeName)) {
                return currentRow.getInt(name);
            } else if ("text".equals(typeName)) {
                return currentRow.getString(name);
            } else if ("timestamp".equals(typeName)) {
                final Instant instant = currentRow.getInstant(name);
                if (instant != null) {
                    return Timestamp.from(instant);
                } else {
                    return null;
                }
            } else if ("uuid".equals(typeName)) {
                return currentRow.getUuid(name);
            } else if ("timeuuid".equals(typeName)) {
                return currentRow.getUuid(name);
            } else if ("varint".equals(typeName)) {
                return currentRow.getInt(name);
            }
        }

        return null;
    }

    @Override
    public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
        final int index = findColumn(columnLabel);
        return getObject(index + 1, type);
    }

    @Override
    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
        final Object returnValue;
        if (type == String.class) {
            returnValue = getString(columnIndex);
        } else if (type == Byte.class) {
            final byte byteValue = getByte(columnIndex);
            returnValue = wasNull() ? null : byteValue;
        } else if (type == Short.class) {
            final short shortValue = getShort(columnIndex);
            returnValue = wasNull() ? null : shortValue;
        } else if (type == Integer.class) {
            final int intValue = getInt(columnIndex);
            returnValue = wasNull() ? null : intValue;
        } else if (type == Long.class) {
            final long longValue = getLong(columnIndex);
            returnValue = wasNull() ? null : longValue;
        } else if (type == BigDecimal.class) {
            returnValue = getBigDecimal(columnIndex);
        } else if (type == Boolean.class) {
            final boolean booleanValue = getBoolean(columnIndex);
            returnValue = wasNull() ? null : booleanValue;
        } else if (type == java.sql.Date.class) {
            returnValue = getDate(columnIndex);
        } else if (type == Time.class) {
            returnValue = getTime(columnIndex);
        } else if (type == Timestamp.class) {
            returnValue = getTimestamp(columnIndex);
        } else if (type == LocalDate.class) {
            returnValue = getLocalDate(columnIndex);
        } else if (type == LocalDateTime.class || type == LocalTime.class) {
            final Timestamp timestamp = getTimestamp(columnIndex, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
            if (timestamp == null) {
                returnValue = null;
            } else {
                final LocalDateTime ldt = LocalDateTime.ofInstant(timestamp.toInstant(), ZoneId.of("UTC"));
                if (type == java.time.LocalDateTime.class) {
                    returnValue = ldt;
                } else {
                    returnValue = ldt.toLocalTime();
                }
            }
        } else if (type == java.time.OffsetDateTime.class) {
            final Timestamp timestamp = getTimestamp(columnIndex);
            if (timestamp == null) {
                returnValue = null;
            } else {
                returnValue = getOffsetDateTime(timestamp);
            }
        } else if (type == java.time.OffsetTime.class) {
            final Time time = getTime(columnIndex);
            if (time == null) {
                returnValue = null;
            } else {
                returnValue = getOffsetTime(time);
            }
        } else if (type == UUID.class) {
            final String uuidString = getString(columnIndex);
            returnValue = wasNull() ? null : UUID.fromString(uuidString);
        } else if (type == SQLXML.class) {
            returnValue = getSQLXML(columnIndex);
        } else if (type == Blob.class) {
            returnValue = getBlob(columnIndex);
        } else if (type == Clob.class) {
            returnValue = getClob(columnIndex);
        } else if (type == NClob.class) {
            returnValue = getNClob(columnIndex);
        } else if (type == byte[].class) {
            returnValue = getBytes(columnIndex);
        } else if (type == Float.class) {
            final float floatValue = getFloat(columnIndex);
            returnValue = wasNull() ? null : floatValue;
        } else if (type == Double.class) {
            final double doubleValue = getDouble(columnIndex);
            returnValue = wasNull() ? null : doubleValue;
        } else {
            throw new SQLException(String.format("Conversion to type %s not supported.", type.getSimpleName()));
        }

        return type.cast(returnValue);
    }

    private OffsetDateTime getOffsetDateTime(final Timestamp timestamp) {
        if (timestamp != null) {
            return OffsetDateTime.ofInstant(timestamp.toInstant(), ZoneId.systemDefault());
        }
        return null;
    }

    private OffsetTime getOffsetTime(final Time time) {
        if (time != null) {
            return time.toLocalTime().atOffset(OffsetTime.now().getOffset());
        }
        return null;
    }

    @Override
    public int getRow() throws SQLException {
        checkNotClosed();
        return rowNumber;
    }

    @Override
    public short getShort(final int index) throws SQLException {
        checkIndex(index);
        return (short) currentRow.getInt(index - 1);
    }

    @Override
    public Set<?> getSet(final int index) throws SQLException {
        checkIndex(index);
        try {
            final SetType setType = (SetType) currentRow.getColumnDefinitions().get(index - 1).getType();
            return currentRow.getSet(index - 1,
                Class.forName(DataTypeEnum.fromCqlTypeName(setType.getElementType().asCql(false, false)).asJavaClass()
                    .getCanonicalName()));
        } catch (ClassNotFoundException e) {
            log.warn("Error while executing getSet()", e);
        }
        return null;
    }

    @Override
    public Set<?> getSet(final String name) throws SQLException {
        checkName(name);
        try {
            final SetType setType = (SetType) currentRow.getColumnDefinitions().get(name).getType();
            return currentRow.getSet(name,
                Class.forName(DataTypeEnum.fromCqlTypeName(setType.getElementType().asCql(false, false)).asJavaClass()
                    .getCanonicalName()));
        } catch (ClassNotFoundException e) {
            log.warn("Error while executing getSet()", e);
        }
        return null;
    }

    @Override
    public short getShort(final String name) throws SQLException {
        checkName(name);
        return (short) currentRow.getInt(name);
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkNotClosed();
        return statement;
    }

    @Override
    public String getString(final int index) throws SQLException {
        checkIndex(index);
        try {
            if (DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().get(index - 1).getType()
                .asCql(false, false)).isCollection()) {
                return getObject(index - 1).toString();
            }
            return currentRow.getString(index - 1);
        } catch (final Exception e) {
            return getObject(index).toString();
        }
    }

    @Override
    public String getString(final String name) throws SQLException {
        checkName(name);
        try {
            if (DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().get(name).getType().asCql(false, false))
                .isCollection()) {
                return getObject(name).toString();
            }
            return currentRow.getString(name);
        } catch (final Exception e) {
            return getObject(name).toString();
        }
    }

    @Override
    public Time getTime(final int index) throws SQLException {
        checkIndex(index);
        final LocalTime localTime = currentRow.getLocalTime(index - 1);
        if (localTime == null)
            return null;
        return Time.valueOf(localTime);
    }

    @Override
    public Time getTime(final int index, final Calendar calendar) throws SQLException {
        checkIndex(index);
        // silently ignore the Calendar argument; it's a hint we do not need
        final LocalTime localTime = currentRow.getLocalTime(index - 1);
        if (localTime == null)
            return null;
        return getTime(index - 1);
    }

    @Override
    public Time getTime(final String name) throws SQLException {
        checkName(name);
        final LocalTime localTime = currentRow.getLocalTime(name);
        if (localTime == null)
            return null;
        return Time.valueOf(localTime);
    }

    @Override
    public Time getTime(final String name, final Calendar calendar) throws SQLException {
        checkName(name);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(name);
    }

    @Override
    public Timestamp getTimestamp(final int index) throws SQLException {
        checkIndex(index);
        final Instant instant = currentRow.getInstant(index - 1);
        if (instant == null)
            return null;
        return Timestamp.from(instant);
    }

    @Override
    public Timestamp getTimestamp(final int index, final Calendar calendar) throws SQLException {
        checkIndex(index);
        // silently ignore the Calendar argument; it's a hint we do not need
        final Instant instant = currentRow.getInstant(index - 1);
        if (instant == null)
            return null;
        return getTimestamp(index - 1);
    }

    @Override
    public Timestamp getTimestamp(final String name) throws SQLException {
        checkName(name);
        final Instant instant = currentRow.getInstant(name);
        if (instant == null)
            return null;
        return Timestamp.from(instant);
    }

    public Timestamp getTimestamp(final String name, final Calendar calendar) throws SQLException {
        checkName(name);
        // silently ignore the Calendar argument; it's a hint we do not need
        return getTimestamp(name);
    }

    @Override
    public int getType() throws SQLException {
        checkNotClosed();
        return resultSetType;
    }

    @Override
    public URL getURL(final int arg0) throws SQLException {
        // URL (awaiting some clarifications as to how it is stored in C* ... just a validated Sting in URL format?
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public URL getURL(final String arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    // The methods below have to be implemented soon.
    // Each set of methods has a more detailed set of issues that should be considered fully...

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkNotClosed();
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkNotClosed();
        return rowNumber == Integer.MAX_VALUE;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkNotClosed();
        return rowNumber == 0;
    }

    @Override
    public boolean isClosed() {
        if (this.statement == null) {
            return true;
        }
        return this.statement.isClosed();
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkNotClosed();
        return rowNumber == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkNotClosed();
        return !rowsIterator.hasNext();
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) {
        return CassandraResultSetExtras.class.isAssignableFrom(iface);
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    // Navigation between rows within the returned set of rows
    // Need to use a list iterator so next() needs completely re-thought
    @Override
    public synchronized boolean next() {
        if (hasMoreRows()) {
            // populateColumns is called upon init to set up the metadata fields; so skip first call
            if (rowNumber != 0) {
                populateColumns();
            }
            rowNumber++;
            return true;
        }
        rowNumber = Integer.MAX_VALUE;
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public boolean relative(final int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    @SuppressWarnings("boxing")
    public void setFetchDirection(final int direction) throws SQLException {
        checkNotClosed();

        if (direction == FETCH_FORWARD || direction == FETCH_REVERSE || direction == FETCH_UNKNOWN) {
            if ((getType() == TYPE_FORWARD_ONLY) && (direction != FETCH_FORWARD)) {
                throw new SQLSyntaxErrorException("attempt to set an illegal direction : " + direction);
            }
            fetchDirection = direction;
        }
        throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
    }

    @Override
    @SuppressWarnings("boxing")
    public void setFetchSize(final int size) throws SQLException {
        checkNotClosed();
        if (size < 0) throw new SQLException(String.format(BAD_FETCH_SIZE, size));
        fetchSize = size;
    }

    @SuppressWarnings("unchecked")
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface.equals(CassandraResultSetExtras.class)) return (T) this;

        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    public boolean wasNull() {
        return wasNull;
    }

    /**
     * RSMD implementation. The metadata returned refers to the column
     * values, not the column names.
     */
    class CResultSetMetaData implements ResultSetMetaData {
        @Override
        public String getCatalogName(final int column) throws SQLException {
            return statement.connection.getCatalog();
        }

        @Override
        public String getColumnClassName(final int column) throws SQLException {
            if (currentRow != null) {
                return DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().get(column - 1).getType()
                    .asCql(false, false)).asJavaClass().getCanonicalName();
            }
            return DataTypeEnum.fromCqlTypeName(driverResultSet.getColumnDefinitions().get(column - 1).getType()
                .asCql(false, false)).asJavaClass().getCanonicalName();

        }

        @Override
        public int getColumnCount() {
            try {
                if (currentRow != null) {
                    return currentRow.getColumnDefinitions().size();
                }
                return driverResultSet.getColumnDefinitions().size();
            } catch (final Exception e) {
                return 0;
            }
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int getColumnDisplaySize(final int column) {
            final ColumnDefinition col;
            if (currentRow != null) {
                col = currentRow.getColumnDefinitions().get(column - 1);
            } else {
                col = driverResultSet.getColumnDefinitions().get(column - 1);
            }
            try {
                int length = -1;
                final AbstractJdbcType jtype = TypesMap.getTypeForComparator(col.getType().toString());
                if (jtype instanceof JdbcBytes) {
                    length = Integer.MAX_VALUE / 2;
                }
                if (jtype instanceof JdbcAscii || jtype instanceof JdbcUTF8) {
                    length = Integer.MAX_VALUE;
                }
                if (jtype instanceof JdbcUUID) {
                    length = 36;
                }
                if (jtype instanceof JdbcInt32) {
                    length = 4;
                }
                if (jtype instanceof JdbcLong) {
                    length = 8;
                }

                return length;
            } catch (final Exception e) {
                return -1;
            }
        }

        @Override
        public String getColumnLabel(final int column) throws SQLException {
            return getColumnName(column);
        }

        @Override
        public String getColumnName(final int column) {
            if (currentRow != null) {
                return currentRow.getColumnDefinitions().get(column - 1).getName().asInternal();
            }
            return driverResultSet.getColumnDefinitions().get(column - 1).getName().asInternal();
        }

        @Override
        public int getColumnType(final int column) {
            final DataType type;
            if (currentRow != null) {
                type = currentRow.getColumnDefinitions().get(column - 1).getType();
            } else {
                type = driverResultSet.getColumnDefinitions().get(column - 1).getType();
            }
            return TypesMap.getTypeForComparator(type.toString()).getJdbcType();
        }

        /**
         * Spec says "database specific type name"; for Cassandra this means the AbstractType.
         */
        public String getColumnTypeName(final int column) {
            final DataType type;
            if (currentRow != null) {
                type = currentRow.getColumnDefinitions().get(column - 1).getType();
            } else {
                type = driverResultSet.getColumnDefinitions().get(column - 1).getType();
            }

            return type.toString();
        }

        public int getPrecision(final int column) {
            return 0;
        }

        public int getScale(final int column) {
            return 0;
        }

        /**
         * return the DEFAULT current Keyspace as the Schema Name
         */
        public String getSchemaName(final int column) throws SQLException {
            //checkIndex(column);
            return statement.connection.getSchema();
        }

        @Override
        public boolean isAutoIncrement(final int column) {
            return true;
        }

        @Override
        public boolean isCaseSensitive(final int column) {
            return true;
        }

        @Override
        public boolean isCurrency(final int column) {
            return false;
        }

        @Override
        public boolean isDefinitelyWritable(final int column) throws SQLException {
            return isWritable(column);
        }

        /**
         * absence is the equivalent of null in Cassandra
         */
        public int isNullable(final int column) {
            return ResultSetMetaData.columnNullable;
        }

        @Override
        public boolean isReadOnly(final int column) {
            return column == 0;
        }

        @Override
        public boolean isSearchable(final int column) {
            return false;
        }

        @Override
        public boolean isSigned(final int column) {
            return false;
        }

        @Override
        public boolean isWrapperFor(final Class<?> iface) {
            return false;
        }

        @Override
        public boolean isWritable(final int column) {
            return column > 0;
        }

        @Override
        public <T> T unwrap(final Class<T> iface) throws SQLException {
            throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
        }

        @Override
        public String getTableName(final int column) {
            String tableName = StringUtils.EMPTY;
            if (currentRow != null) {
                tableName = currentRow.getColumnDefinitions().get(column - 1).getTable().asInternal();
            } else {
                tableName = driverResultSet.getColumnDefinitions().get(column - 1).getTable().asInternal();
            }
            return tableName;
        }
    }

    @Override
    public RowId getRowId(final int columnIndex) {
        return null;
    }

    @Override
    public RowId getRowId(final String columnLabel) {
        return null;
    }

    @Override
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final ByteBuffer byteBuffer = currentRow.getByteBuffer(columnIndex - 1);
        if (byteBuffer != null) {
            final byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes, 0, bytes.length);
            return new ByteArrayInputStream(bytes);
        } else {
            return null;
        }
    }

    @Override
    public InputStream getBinaryStream(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final ByteBuffer byteBuffer = currentRow.getByteBuffer(columnLabel);
        if (byteBuffer != null) {
            final byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes, 0, bytes.length);

            return new ByteArrayInputStream(bytes);
        } else {
            return null;
        }
    }

    @Override
    public Blob getBlob(final int index) throws SQLException {
        checkIndex(index);

        final ByteBuffer byteBuffer = currentRow.getByteBuffer(index - 1);
        if (byteBuffer != null) {
            return new javax.sql.rowset.serial.SerialBlob(byteBuffer.array());
        } else {
            return null;
        }
    }

    @Override
    public Blob getBlob(final String columnName) throws SQLException {
        checkName(columnName);

        final ByteBuffer byteBuffer = currentRow.getByteBuffer(columnName);
        if (byteBuffer != null) {
            return new javax.sql.rowset.serial.SerialBlob(byteBuffer.array());
        } else {
            return null;
        }
    }

}
