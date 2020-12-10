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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ing.data.cassandra.jdbc.Utils.BAD_FETCH_DIR;
import static com.ing.data.cassandra.jdbc.Utils.BAD_FETCH_SIZE;
import static com.ing.data.cassandra.jdbc.Utils.FORWARD_ONLY;
import static com.ing.data.cassandra.jdbc.Utils.MUST_BE_POSITIVE;
import static com.ing.data.cassandra.jdbc.Utils.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.Utils.NO_INTERFACE;
import static com.ing.data.cassandra.jdbc.Utils.VALID_LABELS;
import static com.ing.data.cassandra.jdbc.Utils.WAS_CLOSED_RSLT;

/**
 * A metadata result set.
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
class CassandraMetadataResultSet extends AbstractResultSet implements CassandraResultSetExtras {

    private MetadataRow currentRow;

    /**
     * The rows iterator.
     */
    private Iterator<MetadataRow> rowsIterator;

    int rowNumber = 0;
    // the current row key when iterating through results.
    private byte[] curRowKey = null;

    private final CResultSetMetaData meta;

    private final CassandraStatement statement;

    private int resultSetType;

    private int fetchDirection;

    private int fetchSize;

    private boolean wasNull;

    private MetadataResultSet driverResultSet;

    /**
     * no argument constructor.
     */
    CassandraMetadataResultSet() {
        statement = null;
        meta = new CResultSetMetaData();
    }

    /**
     * Instantiates a new cassandra result set from a com.datastax.driver.core.ResultSet.
     */
    CassandraMetadataResultSet(final CassandraStatement statement, final MetadataResultSet metadataResultSet)
        throws SQLException {
        this.statement = statement;
        this.resultSetType = statement.getResultSetType();
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();
        this.driverResultSet = metadataResultSet;

        rowsIterator = metadataResultSet.iterator();

        // Initialize to column values from the first row.
        // re-Initialize meta-data to column values from the first row (if data exists)
        // NOTE: that the first call to next() will HARMLESSLY re-write these values for the columns
        // NOTE: the row cursor is not advanced and sits before the first row
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
            if (currentRow.getColumnDefinitions() != null) {
                if (index < 1 || index > currentRow.getColumnDefinitions().asList().size()) {
                    throw new SQLSyntaxErrorException(String.format(MUST_BE_POSITIVE, index) + StringUtils.SPACE
                        + currentRow.getColumnDefinitions().asList().size());
                }
            }
        } else if (driverResultSet != null) {
            if (driverResultSet.getColumnDefinitions() != null) {
                if (index < 1 || index > driverResultSet.getColumnDefinitions().asList().size()) {
                    throw new SQLSyntaxErrorException(String.format(MUST_BE_POSITIVE, index) + StringUtils.SPACE
                        + driverResultSet.getColumnDefinitions().asList().size());
                }
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
            if (driverResultSet.getColumnDefinitions() != null) {
                if (!driverResultSet.getColumnDefinitions().contains(name)) {
                    throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
                }
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
        return currentRow.getColumnDefinitions().getIndexOf(name);
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public BigDecimal getBigDecimal(final int index) throws SQLException {
        checkIndex(index);
        return currentRow.getDecimal(index - 1);
    }

    /**
     * @deprecated
     */
    @Override
    @Deprecated
    public BigDecimal getBigDecimal(final int index, final int scale) throws SQLException {
        checkIndex(index);
        return currentRow.getDecimal(index - 1).setScale(scale);
    }

    @Override
    public BigDecimal getBigDecimal(final String name) throws SQLException {
        checkName(name);
        return currentRow.getDecimal(name);
    }

    /**
     * @deprecated
     */
    @Override
    @Deprecated
    public BigDecimal getBigDecimal(final String name, final int scale) throws SQLException {
        checkName(name);
        return currentRow.getDecimal(name).setScale(scale);
    }

    @Override
    public BigInteger getBigInteger(final int index) throws SQLException {
        checkIndex(index);
        return currentRow.getVarint(index - 1);
    }

    @Override
    public BigInteger getBigInteger(final String name) throws SQLException {
        checkName(name);
        return currentRow.getVarint(name);
    }

    @Override
    public boolean getBoolean(final int index) throws SQLException {
        checkIndex(index);
        return currentRow.getBool(index - 1);
    }

    @Override
    public boolean getBoolean(final String name) throws SQLException {
        checkName(name);
        return currentRow.getBool(name);
    }

    @Override
    public byte getByte(final int index) throws SQLException {
        checkIndex(index);
        return currentRow.getBytes(index - 1).get();
    }

    @Override
    public byte getByte(final String name) throws SQLException {
        checkName(name);
        return currentRow.getBytes(name).get();
    }

    @Override
    public byte[] getBytes(final int index) {
        return currentRow.getBytes(index - 1).array();
    }

    @Override
    public byte[] getBytes(final String name) {
        return currentRow.getBytes(name).array();
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkNotClosed();
        return statement.getResultSetConcurrency();
    }

    @Override
    public Date getDate(final int index) throws SQLException {
        checkIndex(index);
        if (currentRow.getDate(index - 1) == null) {
            return null;
        }
        return new java.sql.Date(currentRow.getDate(index - 1).getTime());
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
        if (currentRow.getDate(name) == null) {
            return null;
        }
        return new java.sql.Date(currentRow.getDate(name).getTime());
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
        if ("float".equals(currentRow.getColumnDefinitions().getType(index - 1).asCql(false, false))) {
            return currentRow.getFloat(index - 1);
        }
        return currentRow.getDouble(index - 1);
    }

    @Override
    public double getDouble(final String name) throws SQLException {
        checkName(name);
        if ("float".equals(currentRow.getColumnDefinitions().getType(name).asCql(false, false))) {
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
        if (DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().getType(index - 1).asCql(false, false))
            .isCollection()) {
            final ListType listType = (ListType) currentRow.getColumnDefinitions().getType(index - 1);
            final Class<?> itemsClass = DataTypeEnum.fromCqlTypeName(listType.getElementType()
                .asCql(false, false)).javaType;
            return Lists.newArrayList(currentRow.getList(index - 1, itemsClass));
        }
        return currentRow.getList(index - 1, String.class);
    }

    @Override
    public List<?> getList(final String name) throws SQLException {
        checkName(name);
        if (DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().getType(name).asCql(false, false))
            .isCollection()) {
            final ListType listType = (ListType) currentRow.getColumnDefinitions().getType(name);
            final Class<?> itemsClass = DataTypeEnum.fromCqlTypeName(listType.getElementType()
                .asCql(false, false)).javaType;
            return Lists.newArrayList(currentRow.getList(name, itemsClass));
        }
        return currentRow.getList(name, String.class);
    }

    @Override
    public long getLong(final int index) throws SQLException {
        checkIndex(index);
        if ("int".equals(currentRow.getColumnDefinitions().getType(index - 1).asCql(false, false))) {
            return currentRow.getInt(index - 1);
        } else if ("varint".equals(currentRow.getColumnDefinitions().getType(index - 1).asCql(false, false))) {
            return currentRow.getVarint(index - 1).longValue();
        } else {
            return currentRow.getLong(index - 1);
        }
    }

    @Override
    public long getLong(final String name) throws SQLException {
        checkName(name);
        if ("int".equals(currentRow.getColumnDefinitions().getType(name).asCql(false, false))) {
            return currentRow.getInt(name);
        } else if ("varint".equals(currentRow.getColumnDefinitions().getType(name).asCql(false, false))) {
            return currentRow.getVarint(name).longValue();
        } else {
            return currentRow.getLong(name);
        }
    }

    @Override
    public Map<?, ?> getMap(final int index) throws SQLException {
        checkIndex(index);
        wasNull = currentRow.isNull(index - 1);

        if (DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().getType(index - 1).asCql(false, false))
            .isCollection()) {
            final MapType mapType = (MapType) currentRow.getColumnDefinitions().getType(index - 1);
            final Class<?> keysClass = DataTypeEnum.fromCqlTypeName(mapType.getKeyType()
                .asCql(false, false)).javaType;
            final Class<?> valuesClass = DataTypeEnum.fromCqlTypeName(mapType.getValueType()
                .asCql(false, false)).javaType;
            return Maps.newLinkedHashMap(currentRow.getMap(index - 1, keysClass, valuesClass));
        }
        return currentRow.getMap(index - 1, String.class, String.class);
    }

    @Override
    public Map<?, ?> getMap(final String name) throws SQLException {
        checkName(name);

        if (DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().getType(name).asCql(false, false))
            .isCollection()) {
            final MapType mapType = (MapType) currentRow.getColumnDefinitions().getType(name);
            final Class<?> keysClass = DataTypeEnum.fromCqlTypeName(mapType.getKeyType()
                .asCql(false, false)).javaType;
            final Class<?> valuesClass = DataTypeEnum.fromCqlTypeName(mapType.getValueType()
                .asCql(false, false)).javaType;
            return Maps.newLinkedHashMap(currentRow.getMap(name, keysClass, valuesClass));
        }
        return currentRow.getMap(name, String.class, String.class);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkNotClosed();
        return meta;
    }

    @Override
    @SuppressWarnings("boxing")
    public Object getObject(final int index) throws SQLException {
        checkIndex(index);

        final String typeName = currentRow.getColumnDefinitions().getType(index - 1).asCql(false, false);
        if ("varchar".equals(typeName)) {
            return currentRow.getString(index - 1);
        } else if ("ascii".equals(typeName)) {
            return currentRow.getString(index - 1);
        } else if ("integer".equals(typeName)) {
            return currentRow.getInt(index - 1);
        } else if ("bigint".equals(typeName)) {
            return currentRow.getLong(index - 1);
        } else if ("blob".equals(typeName)) {
            return currentRow.getBytes(index - 1);
        } else if ("boolean".equals(typeName)) {
            return currentRow.getBool(index - 1);
        } else if ("counter".equals(typeName)) {
            return currentRow.getLong(index - 1);
        } else if ("decimal".equals(typeName)) {
            return currentRow.getDecimal(index - 1);
        } else if ("double".equals(typeName)) {
            return currentRow.getDouble(index - 1);
        } else if ("float".equals(typeName)) {
            return currentRow.getFloat(index - 1);
        } else if ("inet".equals(typeName)) {
            return currentRow.getInet(index - 1);
        } else if ("int".equals(typeName)) {
            return currentRow.getInt(index - 1);
        } else if ("text".equals(typeName)) {
            return currentRow.getString(index - 1);
        } else if ("timestamp".equals(typeName)) {
            return new Timestamp((currentRow.getDate(index - 1)).getTime());
        } else if ("uuid".equals(typeName)) {
            return currentRow.getUUID(index - 1);
        } else if ("timeuuid".equals(typeName)) {
            return currentRow.getUUID(index - 1);
        } else if ("varint".equals(typeName)) {
            return currentRow.getInt(index - 1);
        }

        return null;
    }

    @Override
    @SuppressWarnings("boxing")
    public Object getObject(final String name) throws SQLException {
        checkName(name);

        final String typeName = currentRow.getColumnDefinitions().getType(name).asCql(false, false);
        if ("varchar".equals(typeName)) {
            return currentRow.getString(name);
        } else if ("ascii".equals(typeName)) {
            return currentRow.getString(name);
        } else if ("integer".equals(typeName)) {
            return currentRow.getInt(name);
        } else if ("bigint".equals(typeName)) {
            return currentRow.getLong(name);
        } else if ("blob".equals(typeName)) {
            return currentRow.getBytes(name);
        } else if ("boolean".equals(typeName)) {
            return currentRow.getBool(name);
        } else if ("counter".equals(typeName)) {
            return currentRow.getLong(name);
        } else if ("decimal".equals(typeName)) {
            return currentRow.getDecimal(name);
        } else if ("double".equals(typeName)) {
            return currentRow.getDouble(name);
        } else if ("float".equals(typeName)) {
            return currentRow.getFloat(name);
        } else if ("inet".equals(typeName)) {
            return currentRow.getInet(name);
        } else if ("int".equals(typeName)) {
            return currentRow.getInt(name);
        } else if ("text".equals(typeName)) {
            return currentRow.getString(name);
        } else if ("timestamp".equals(typeName)) {
            return new Timestamp((currentRow.getDate(name)).getTime());
        } else if ("uuid".equals(typeName)) {
            return currentRow.getUUID(name);
        } else if ("timeuuid".equals(typeName)) {
            return currentRow.getUUID(name);
        } else if ("varint".equals(typeName)) {
            return currentRow.getInt(name);
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
            final SetType setType = (SetType) currentRow.getColumnDefinitions().getType(index - 1);
            return currentRow.getSet(index - 1,
                Class.forName(DataTypeEnum.fromCqlTypeName(setType.asCql(false, false)).asJavaClass()
                    .getCanonicalName()));
        } catch (final ClassNotFoundException e) {
            throw new SQLNonTransientException(e);
        }

    }

    @Override
    public Set<?> getSet(final String name) throws SQLException {
        checkName(name);
        try {
            final SetType setType = (SetType) currentRow.getColumnDefinitions().getType(name);
            return currentRow.getSet(name,
                Class.forName(DataTypeEnum.fromCqlTypeName(setType.asCql(false, false)).asJavaClass()
                    .getCanonicalName()));
        } catch (final ClassNotFoundException e) {
            throw new SQLNonTransientException(e);
        }

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
            if (DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().getType(index - 1).asCql(false, false))
                .isCollection()) {
                return getObject(index).toString();
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
            if (DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().getType(name).asCql(false, false))
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
        final java.util.Date date = currentRow.getDate(index - 1);
        if (date == null) {
            return null;
        }
        return new Time(currentRow.getDate(index - 1).getTime());
    }

    @Override
    public Time getTime(final int index, final Calendar calendar) throws SQLException {
        checkIndex(index);
        // silently ignore the Calendar argument; it's a hint we do not need
        final java.util.Date date = currentRow.getDate(index - 1);
        if (date == null) {
            return null;
        }
        return getTime(index - 1);
    }

    @Override
    public Time getTime(final String name) throws SQLException {
        checkName(name);
        final java.util.Date date = currentRow.getDate(name);
        if (date == null) {
            return null;
        }
        return new Time(currentRow.getDate(name).getTime());
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
        final java.util.Date date = currentRow.getDate(index - 1);
        if (date == null) {
            return null;
        }
        return new Timestamp(currentRow.getDate(index - 1).getTime());
    }

    @Override
    public Timestamp getTimestamp(final int index, final Calendar calendar) throws SQLException {
        checkIndex(index);
        // silently ignore the Calendar argument; it's a hint we do not need
        final java.util.Date date = currentRow.getDate(index - 1);
        if (date == null) {
            return null;
        }
        return getTimestamp(index - 1);
    }

    @Override
    public Timestamp getTimestamp(final String name) throws SQLException {
        checkName(name);
        final java.util.Date date = currentRow.getDate(name);
        if (date == null) {
            return null;
        }
        return new Timestamp(currentRow.getDate(name).getTime());
    }

    @Override
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
                throw new SQLSyntaxErrorException("Attempt to set an illegal direction: " + direction);
            }
            fetchDirection = direction;
        }
        throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
    }

    @Override
    @SuppressWarnings("boxing")
    public void setFetchSize(final int size) throws SQLException {
        checkNotClosed();
        if (size < 0) {
            throw new SQLException(String.format(BAD_FETCH_SIZE, size));
        }
        fetchSize = size;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface.equals(CassandraResultSetExtras.class)) {
            return (T) this;
        }
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    /**
     * RSMD implementation. The metadata returned refers to the column values, not the column names.
     */
    class CResultSetMetaData implements ResultSetMetaData {
        @Override
        public String getCatalogName(final int column) throws SQLException {
            return statement.connection.getCatalog();
        }

        @Override
        public String getColumnClassName(final int column) {
            if (currentRow != null) {
                return currentRow.getColumnDefinitions().getType(column - 1).asCql(false, false);
            }
            return driverResultSet.getColumnDefinitions().asList().get(column - 1).getType().asCql(false, false);
        }

        @Override
        public int getColumnCount() {
            if (currentRow != null) {
                return currentRow.getColumnDefinitions().size();
            }
            return (driverResultSet != null && driverResultSet.getColumnDefinitions() != null) ?
                driverResultSet.getColumnDefinitions().size() : 0;
        }

        @SuppressWarnings("rawtypes")
        public int getColumnDisplaySize(final int column) {
            try {
                final AbstractJdbcType jtype;
                final ColumnDefinitions.Definition col;
                if (currentRow != null) {
                    col = currentRow.getColumnDefinitions().asList().get(column - 1);
                } else {
                    col = driverResultSet.getColumnDefinitions().asList().get(column - 1);
                }
                jtype = TypesMap.getTypeForComparator(col.getType().toString());

                int length = -1;

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
            try {
                if (currentRow != null) {
                    return currentRow.getColumnDefinitions().getName(column - 1);
                }
                return driverResultSet.getColumnDefinitions().asList().get(column - 1).getName();
            } catch (final Exception e) {
                return StringUtils.EMPTY;
            }
        }

        @Override
        public int getColumnType(final int column) {
            final DataType type;
            if (currentRow != null) {
                type = currentRow.getColumnDefinitions().getType(column - 1);
            } else {
                type = driverResultSet.getColumnDefinitions().asList().get(column - 1).getType();
            }
            return TypesMap.getTypeForComparator(type.toString()).getJdbcType();
        }

        /**
         * Spec says "database specific type name"; for Cassandra this means the AbstractType.
         */
        public String getColumnTypeName(final int column) {
            final DataType type;
            try {
                if (currentRow != null) {
                    type = currentRow.getColumnDefinitions().getType(column - 1);
                } else {
                    type = driverResultSet.getColumnDefinitions().getType(column - 1);
                }
                return type.toString();
            } catch (final Exception e) {
                return "VARCHAR";
            }
        }

        @Override
        public int getPrecision(final int column) {
            return 0;
        }

        @Override
        public int getScale(final int column) {
            return 0;
        }

        /**
         * return the DEFAULT current Keyspace as the Schema Name
         */
        public String getSchemaName(final int column) throws SQLException {
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
        @Override
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
                tableName = currentRow.getColumnDefinitions().getTable(column - 1);
            } else {
                tableName = driverResultSet.getColumnDefinitions().getTable(column - 1);
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
        final byte[] bytes = new byte[currentRow.getBytes(columnIndex - 1).remaining()];
        currentRow.getBytes(columnIndex - 1).get(bytes, 0, bytes.length);
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public InputStream getBinaryStream(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final byte[] bytes = new byte[currentRow.getBytes(columnLabel).remaining()];
        currentRow.getBytes(columnLabel).get(bytes, 0, bytes.length);
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public Blob getBlob(final int index) throws SQLException {
        checkIndex(index);
        return new javax.sql.rowset.serial.SerialBlob(currentRow.getBytes(index - 1).array());
    }

    @Override
    public Blob getBlob(final String columnName) throws SQLException {
        checkName(columnName);
        return new javax.sql.rowset.serial.SerialBlob(currentRow.getBytes(columnName).array());
    }

}
