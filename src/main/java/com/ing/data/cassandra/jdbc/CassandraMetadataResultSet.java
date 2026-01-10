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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.ing.data.cassandra.jdbc.metadata.MetadataResultSet;
import com.ing.data.cassandra.jdbc.metadata.MetadataRow;
import com.ing.data.cassandra.jdbc.types.AbstractJdbcType;
import com.ing.data.cassandra.jdbc.types.DataTypeEnum;
import com.ing.data.cassandra.jdbc.types.TypesMap;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ing.data.cassandra.jdbc.types.AbstractJdbcType.DEFAULT_PRECISION;
import static com.ing.data.cassandra.jdbc.types.AbstractJdbcType.DEFAULT_SCALE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_FETCH_DIR;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_FETCH_SIZE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.ILLEGAL_FETCH_DIRECTION_FOR_FORWARD_ONLY;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.MALFORMED_URL;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.MUST_BE_POSITIVE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NO_INTERFACE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.UNABLE_TO_RETRIEVE_METADATA;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.VALID_LABELS;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.WAS_CLOSED_RS;
import static java.lang.String.format;

/**
 * Cassandra metadata result set. This is an implementation of {@link ResultSet} for database metadata.
 * <p>
 *     It also implements {@link CassandraResultSetExtras} interface providing extra methods not defined in JDBC API to
 *     better handle some CQL data types.
 * </p>
 * <p>
 *     The supported data types in CQL are:
 *     <table border="1">
 *         <tr><th>CQL Type </th><th>Java type          </th><th>Description</th></tr>
 *         <tr><td>ascii    </td><td>{@link String}     </td><td>US-ASCII character string</td></tr>
 *         <tr><td>bigint   </td><td>{@link Long}       </td><td>64-bit signed long</td></tr>
 *         <tr><td>blob     </td><td>{@link ByteBuffer} </td><td>Arbitrary bytes (no validation)</td></tr>
 *         <tr><td>boolean  </td><td>{@link Boolean}    </td><td>Boolean value: true or false</td></tr>
 *         <tr><td>counter  </td><td>{@link Long}       </td><td>Counter column (64-bit long)</td></tr>
 *         <tr><td>date     </td><td>{@link Date}       </td><td>A date with no corresponding time value; encoded date
 *         as a 32-bit integer representing days since epoch (January 1, 1970)</td></tr>
 *         <tr><td>decimal  </td><td>{@link BigDecimal} </td><td>Variable-precision decimal</td></tr>
 *         <tr><td>double   </td><td>{@link Double}     </td><td>64-bit IEEE-754 floating point</td></tr>
 *         <tr><td>duration </td><td>{@link CqlDuration}</td><td>A duration with nanosecond precision</td></tr>
 *         <tr><td>float    </td><td>{@link Float}      </td><td>32-bit IEEE-754 floating point</td></tr>
 *         <tr><td>inet     </td><td>{@link InetAddress}</td><td>IP address string in IPv4 or IPv6 format</td></tr>
 *         <tr><td>int      </td><td>{@link Integer}    </td><td>32-bit signed integer</td></tr>
 *         <tr><td>list     </td><td>{@link List}       </td><td>A collection of one or more ordered elements:
 *         <code>[literal, literal, literal]</code></td></tr>
 *         <tr><td>map      </td><td>{@link Map}        </td><td>A JSON-style array of literals:
 *         <code>{ literal : literal, literal : literal ... }</code></td></tr>
 *         <tr><td>set      </td><td>{@link Set}        </td><td>A collection of one or more elements:
 *         <code>{ literal, literal, literal }</code></td></tr>
 *         <tr><td>smallint </td><td>{@link Short}      </td><td>16-bit signed integer</td></tr>
 *         <tr><td>text     </td><td>{@link String}     </td><td>UTF-8 encoded string</td></tr>
 *         <tr><td>time     </td><td>{@link Time}       </td><td>A value encoded as a 64-bit signed integer
 *         representing the number of nanoseconds since midnight</td></tr>
 *         <tr><td>timestamp</td><td>{@link Timestamp}  </td><td>Date and time with millisecond precision, encoded as
 *         8 bytes since epoch</td></tr>
 *         <tr><td>timeuuid </td><td>{@link UUID}       </td><td>Version 1 UUID only</td></tr>
 *         <tr><td>tinyint  </td><td>{@link Byte}       </td><td>8-bits signed integer</td></tr>
 *         <tr><td>uuid     </td><td>{@link UUID}       </td><td>A UUID in standard UUID format</td></tr>
 *         <tr><td>varchar  </td><td>{@link String}     </td><td>UTF-8 encoded string</td></tr>
 *         <tr><td>varint   </td><td>{@link BigInteger} </td><td>Arbitrary-precision integer</td></tr>
 *     </table>
 *     See: <a href="https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/cql_data_types_c.html">
 *         CQL data types reference</a> and
 *     <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/temporal_types/">
 *         CQL temporal types reference</a>.
 * </p>
 *
 * @see java.sql.DatabaseMetaData
 * @see CassandraDatabaseMetaData
 */
public class CassandraMetadataResultSet extends AbstractResultSet implements CassandraResultSetExtras {

    int rowNumber = 0;
    // Metadata of this result set.
    private final CResultSetMetaData metadata;
    private final CassandraStatement statement;
    private MetadataRow currentRow;
    private Iterator<MetadataRow> rowsIterator;
    private int resultSetType;
    private int fetchDirection;
    private int fetchSize;
    private boolean wasNull;
    private boolean isClosed;
    // Result set from the Cassandra driver.
    private MetadataResultSet driverResultSet;

    /**
     * No argument constructor.
     */
    CassandraMetadataResultSet() {
        this.metadata = new CResultSetMetaData();
        this.statement = null;
        this.isClosed = false;
    }

    /**
     * Constructor. It instantiates a new Cassandra metadata result set from a {@link MetadataResultSet}.
     *
     * @param statement         The statement.
     * @param metadataResultSet The metadata result set from the Cassandra driver.
     * @throws SQLException if a database access error occurs or this constructor is called with a closed
     * {@link Statement}.
     */
    CassandraMetadataResultSet(final CassandraStatement statement,
                               final MetadataResultSet metadataResultSet) throws SQLException {
        this.metadata = new CResultSetMetaData();
        this.statement = statement;
        this.resultSetType = statement.getResultSetType();
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();
        this.driverResultSet = metadataResultSet;
        this.rowsIterator = metadataResultSet.iterator();
        this.isClosed = false;

        // Initialize the column values from the first row.
        // Note that the first call to next() will harmlessly re-write these values for the columns. The row cursor
        // is not moved forward and stays before the first row.
        if (hasMoreRows()) {
            populateColumns();
        }
    }

    /**
     * Builds a new instance of Cassandra metadata result set from a {@link MetadataResultSet}.
     *
     * @param statement         The statement.
     * @param metadataResultSet The metadata result set from the Cassandra driver.
     * @return A new instance of Cassandra metadata result set.
     * @throws SQLException if a database access error occurs or this constructor is called with a closed
     * {@link Statement}.
     */
    public static CassandraMetadataResultSet buildFrom(final CassandraStatement statement,
                                                       final MetadataResultSet metadataResultSet) throws SQLException {
        return new CassandraMetadataResultSet(statement, metadataResultSet);
    }

    private void populateColumns() {
        this.currentRow = this.rowsIterator.next();
    }

    @Override
    DataType getCqlDataType(final int columnIndex) throws SQLException {
        if (this.currentRow != null && this.currentRow.getColumnDefinitions() != null) {
            return this.currentRow.getColumnDefinitions().getType(columnIndex - 1);
        }
        if (this.driverResultSet != null && this.driverResultSet.getColumnDefinitions() != null) {
            return this.driverResultSet.getColumnDefinitions().getType(columnIndex - 1);
        }
        throw new SQLException(UNABLE_TO_RETRIEVE_METADATA);
    }

    @Override
    DataType getCqlDataType(final String columnLabel) throws SQLException {
        if (this.currentRow != null && this.currentRow.getColumnDefinitions() != null) {
            return this.currentRow.getColumnDefinitions().getType(columnLabel);
        }
        if (this.driverResultSet != null && this.driverResultSet.getColumnDefinitions() != null) {
            return this.driverResultSet.getColumnDefinitions().getType(columnLabel);
        }
        throw new SQLException(UNABLE_TO_RETRIEVE_METADATA);
    }

    private void checkIndex(final int index) throws SQLException {
        if (this.currentRow != null) {
            if (this.currentRow.getColumnDefinitions() != null) {
                if (index < 1 || index > this.currentRow.getColumnDefinitions().asList().size()) {
                    throw new SQLSyntaxErrorException(format(MUST_BE_POSITIVE, index) + StringUtils.SPACE
                        + this.currentRow.getColumnDefinitions().asList().size());
                }
            }
            this.wasNull = this.currentRow.isNull(index - 1);
        } else if (this.driverResultSet != null) {
            if (this.driverResultSet.getColumnDefinitions() != null) {
                if (index < 1 || index > this.driverResultSet.getColumnDefinitions().asList().size()) {
                    throw new SQLSyntaxErrorException(format(MUST_BE_POSITIVE, index) + StringUtils.SPACE
                        + this.driverResultSet.getColumnDefinitions().asList().size());
                }
            }
        }
    }

    private void checkName(final String name) throws SQLException {
        if (this.currentRow != null) {
            if (!this.currentRow.getColumnDefinitions().contains(name)) {
                throw new SQLSyntaxErrorException(format(VALID_LABELS, name));
            }
            this.wasNull = this.currentRow.isNull(name);
        } else if (this.driverResultSet != null) {
            if (this.driverResultSet.getColumnDefinitions() != null) {
                if (!this.driverResultSet.getColumnDefinitions().contains(name)) {
                    throw new SQLSyntaxErrorException(format(VALID_LABELS, name));
                }
            }
        }
    }

    private void checkNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLRecoverableException(WAS_CLOSED_RS);
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        // This implementation does not support the collection of warnings so clearing is a no-op, but it still throws
        // an exception when called on a closed result set.
        checkNotClosed();
    }

    @Override
    public void close() throws SQLException {
        if (!isClosed()) {
            this.isClosed = true;
        }
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
        checkNotClosed();
        checkName(columnLabel);
        if (this.currentRow != null) {
            return this.currentRow.getColumnDefinitions().getIndexOf(columnLabel) + 1;
        } else if (this.driverResultSet != null && this.driverResultSet.getColumnDefinitions() != null) {
            return this.driverResultSet.getColumnDefinitions().getIndexOf(columnLabel) + 1;
        }
        throw new SQLSyntaxErrorException(format(VALID_LABELS, columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        return this.currentRow.getDecimal(columnIndex - 1);
    }

    /**
     * @deprecated use {@link #getBigDecimal(int)}.
     */
    @Override
    @Deprecated
    public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
        checkIndex(columnIndex);
        final BigDecimal decimalValue = this.currentRow.getDecimal(columnIndex - 1);
        if (decimalValue == null) {
            return null;
        } else {
            return decimalValue.setScale(scale, RoundingMode.HALF_UP);
        }
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        return this.currentRow.getDecimal(columnLabel);
    }

    /**
     * @deprecated use {@link #getBigDecimal(String)}.
     */
    @Override
    @Deprecated
    public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
        checkName(columnLabel);
        final BigDecimal decimalValue = this.currentRow.getDecimal(columnLabel);
        if (decimalValue == null) {
            return null;
        } else {
            return decimalValue.setScale(scale, RoundingMode.HALF_UP);
        }
    }

    @Override
    public BigInteger getBigInteger(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        return this.currentRow.getVarint(columnIndex - 1);
    }

    @Override
    public BigInteger getBigInteger(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        return this.currentRow.getVarint(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final ByteBuffer byteBuffer = this.currentRow.getBytes(columnIndex - 1);
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
        final ByteBuffer byteBuffer = this.currentRow.getBytes(columnLabel);
        if (byteBuffer != null) {
            final byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes, 0, bytes.length);
            return new ByteArrayInputStream(bytes);
        } else {
            return null;
        }
    }

    @Override
    public Blob getBlob(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final ByteBuffer byteBuffer = this.currentRow.getBytes(columnIndex - 1);
        if (byteBuffer != null) {
            return new javax.sql.rowset.serial.SerialBlob(byteBuffer.array());
        } else {
            return null;
        }
    }

    @Override
    public Blob getBlob(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final ByteBuffer byteBuffer = this.currentRow.getBytes(columnLabel);
        if (byteBuffer != null) {
            return new javax.sql.rowset.serial.SerialBlob(byteBuffer.array());
        } else {
            return null;
        }
    }

    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        return this.currentRow.getBool(columnIndex - 1);
    }

    @Override
    public boolean getBoolean(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        return this.currentRow.getBool(columnLabel);
    }

    @Override
    public byte getByte(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        return this.currentRow.getByte(columnIndex - 1);
    }

    @Override
    public byte getByte(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        return this.currentRow.getByte(columnLabel);
    }

    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final ByteBuffer byteBuffer = this.currentRow.getBytes(columnIndex - 1);
        if (byteBuffer != null) {
            return byteBuffer.array();
        }
        return null;
    }

    @Override
    public byte[] getBytes(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final ByteBuffer byteBuffer = this.currentRow.getBytes(columnLabel);
        if (byteBuffer != null) {
            return byteBuffer.array();
        }
        return null;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkNotClosed();
        return this.statement.getResultSetConcurrency();
    }

    @Override
    public Date getDate(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final java.util.Date dateValue = this.currentRow.getDate(columnIndex - 1);
        if (dateValue == null) {
            return null;
        }
        return new Date(dateValue.getTime());
    }

    @Override
    public Date getDate(final int columnIndex, final Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; it's a hint we do not need
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final java.util.Date dateValue = this.currentRow.getDate(columnLabel);
        if (dateValue == null) {
            return null;
        }
        return new Date(dateValue.getTime());
    }

    @Override
    public Date getDate(final String columnLabel, final Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; it's a hint we do not need
        return getDate(columnLabel);
    }

    @Override
    public double getDouble(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        if (isCqlType(columnIndex, DataTypeEnum.FLOAT)) {
            return this.currentRow.getFloat(columnIndex - 1);
        }
        return this.currentRow.getDouble(columnIndex - 1);
    }

    @Override
    public double getDouble(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        if (isCqlType(columnLabel, DataTypeEnum.FLOAT)) {
            return this.currentRow.getFloat(columnLabel);
        }
        return this.currentRow.getDouble(columnLabel);
    }

    @Override
    public CqlDuration getDuration(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        return this.currentRow.getDuration(columnIndex - 1);
    }

    @Override
    public CqlDuration getDuration(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        return this.currentRow.getDuration(columnLabel);
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getFetchDirection() throws SQLException {
        checkNotClosed();
        return this.fetchDirection;
    }

    @Override
    public void setFetchDirection(final int direction) throws SQLException {
        checkNotClosed();
        if (direction == FETCH_FORWARD || direction == FETCH_REVERSE || direction == FETCH_UNKNOWN) {
            if (getType() == TYPE_FORWARD_ONLY && direction != FETCH_FORWARD) {
                throw new SQLSyntaxErrorException(format(ILLEGAL_FETCH_DIRECTION_FOR_FORWARD_ONLY, direction));
            }
            this.fetchDirection = direction;
        }
        throw new SQLSyntaxErrorException(format(BAD_FETCH_DIR, direction));
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkNotClosed();
        return this.fetchSize;
    }

    @Override
    public void setFetchSize(final int size) throws SQLException {
        checkNotClosed();
        if (size < 0) {
            throw new SQLException(format(BAD_FETCH_SIZE, size));
        }
        this.fetchSize = size;
    }

    @Override
    public float getFloat(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        return this.currentRow.getFloat(columnIndex - 1);
    }

    @Override
    public float getFloat(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        return this.currentRow.getFloat(columnLabel);
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getHoldability() throws SQLException {
        checkNotClosed();
        return this.statement.getResultSetHoldability();
    }

    @Override
    public int getInt(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        return this.currentRow.getInt(columnIndex - 1);
    }

    @Override
    public int getInt(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        return this.currentRow.getInt(columnLabel);
    }

    @Override
    public List<?> getList(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final DataType cqlDataType = getCqlDataType(columnIndex);

        if (DataTypeEnum.fromCqlTypeName(cqlDataType.asCql(false, false)).isCollection()) {
            final ListType listType = (ListType) cqlDataType;
            final Class<?> itemsClass = DataTypeEnum.fromCqlTypeName(listType.getElementType()
                .asCql(false, false)).javaType;
            final List<?> resultList = this.currentRow.getList(columnIndex - 1, itemsClass);
            if (resultList == null) {
                return null;
            }
            return new ArrayList<>(resultList);
        }
        return this.currentRow.getList(columnIndex - 1, String.class);
    }

    @Override
    public List<?> getList(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final DataType cqlDataType = getCqlDataType(columnLabel);

        if (DataTypeEnum.fromCqlTypeName(cqlDataType.asCql(false, false)).isCollection()) {
            final ListType listType = (ListType) cqlDataType;
            final Class<?> itemsClass = DataTypeEnum.fromCqlTypeName(listType.getElementType()
                .asCql(false, false)).javaType;
            final List<?> resultList = this.currentRow.getList(columnLabel, itemsClass);
            if (resultList == null) {
                return null;
            }
            return new ArrayList<>(resultList);
        }
        return this.currentRow.getList(columnLabel, String.class);
    }

    @Override
    public long getLong(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        if (isCqlType(columnIndex, DataTypeEnum.INT)) {
            return this.currentRow.getInt(columnIndex - 1);
        } else if (isCqlType(columnIndex, DataTypeEnum.VARINT)) {
            return this.currentRow.getVarint(columnIndex - 1).longValue();
        } else {
            return this.currentRow.getLong(columnIndex - 1);
        }
    }

    @Override
    public long getLong(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        if (isCqlType(columnLabel, DataTypeEnum.INT)) {
            return this.currentRow.getInt(columnLabel);
        } else if (isCqlType(columnLabel, DataTypeEnum.VARINT)) {
            return this.currentRow.getVarint(columnLabel).longValue();
        } else {
            return this.currentRow.getLong(columnLabel);
        }
    }

    @Override
    public Map<?, ?> getMap(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final DataType cqlDataType = getCqlDataType(columnIndex);

        if (DataTypeEnum.fromCqlTypeName(cqlDataType.asCql(false, false)).isCollection()) {
            final MapType mapType = (MapType) cqlDataType;
            final Class<?> keysClass = DataTypeEnum.fromCqlTypeName(mapType.getKeyType()
                .asCql(false, false)).javaType;
            final Class<?> valuesClass = DataTypeEnum.fromCqlTypeName(mapType.getValueType()
                .asCql(false, false)).javaType;
            return new LinkedHashMap<>(this.currentRow.getMap(columnIndex - 1, keysClass, valuesClass));
        }
        return this.currentRow.getMap(columnIndex - 1, String.class, String.class);
    }

    @Override
    public Map<?, ?> getMap(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final DataType cqlDataType = getCqlDataType(columnLabel);

        if (DataTypeEnum.fromCqlTypeName(cqlDataType.asCql(false, false)).isCollection()) {
            final MapType mapType = (MapType) cqlDataType;
            final Class<?> keysClass = DataTypeEnum.fromCqlTypeName(mapType.getKeyType()
                .asCql(false, false)).javaType;
            final Class<?> valuesClass = DataTypeEnum.fromCqlTypeName(mapType.getValueType()
                .asCql(false, false)).javaType;
            return new LinkedHashMap<>(this.currentRow.getMap(columnLabel, keysClass, valuesClass));
        }
        return this.currentRow.getMap(columnLabel, String.class, String.class);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkNotClosed();
        return this.metadata;
    }

    @Override
    public Object getObject(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final DataTypeEnum dataType = DataTypeEnum.fromCqlTypeName(getCqlDataType(columnIndex).asCql(false, false));

        return switch (dataType) {
            case VARCHAR, ASCII, TEXT -> this.currentRow.getString(columnIndex - 1);
            case VARINT -> this.currentRow.getVarint(columnIndex - 1);
            case INT -> this.currentRow.getInt(columnIndex - 1);
            case SMALLINT -> this.currentRow.getShort(columnIndex - 1);
            case TINYINT -> this.currentRow.getByte(columnIndex - 1);
            case BIGINT, COUNTER -> this.currentRow.getLong(columnIndex - 1);
            case BLOB -> this.currentRow.getBytes(columnIndex - 1);
            case BOOLEAN -> this.currentRow.getBool(columnIndex - 1);
            case DATE -> this.currentRow.getDate(columnIndex - 1);
            case TIME -> this.currentRow.getTime(columnIndex - 1);
            case DECIMAL -> this.currentRow.getDecimal(columnIndex - 1);
            case DOUBLE -> this.currentRow.getDouble(columnIndex - 1);
            case FLOAT -> this.currentRow.getFloat(columnIndex - 1);
            case INET -> this.currentRow.getInet(columnIndex - 1);
            case TIMESTAMP -> new Timestamp((currentRow.getDate(columnIndex - 1)).getTime());
            case DURATION -> this.currentRow.getDuration(columnIndex - 1);
            case UUID, TIMEUUID -> this.currentRow.getUUID(columnIndex - 1);
            default -> null;
        };
    }

    @Override
    public Object getObject(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final DataTypeEnum dataType = DataTypeEnum.fromCqlTypeName(getCqlDataType(columnLabel).asCql(false, false));

        return switch (dataType) {
            case VARCHAR, ASCII, TEXT -> this.currentRow.getString(columnLabel);
            case VARINT -> this.currentRow.getVarint(columnLabel);
            case INT -> this.currentRow.getInt(columnLabel);
            case SMALLINT -> this.currentRow.getShort(columnLabel);
            case TINYINT -> this.currentRow.getByte(columnLabel);
            case BIGINT, COUNTER -> this.currentRow.getLong(columnLabel);
            case BLOB -> this.currentRow.getBytes(columnLabel);
            case BOOLEAN -> this.currentRow.getBool(columnLabel);
            case DATE -> this.currentRow.getDate(columnLabel);
            case TIME -> this.currentRow.getTime(columnLabel);
            case DECIMAL -> this.currentRow.getDecimal(columnLabel);
            case DOUBLE -> this.currentRow.getDouble(columnLabel);
            case FLOAT -> this.currentRow.getFloat(columnLabel);
            case INET -> this.currentRow.getInet(columnLabel);
            case TIMESTAMP -> new Timestamp((this.currentRow.getDate(columnLabel)).getTime());
            case DURATION -> this.currentRow.getDuration(columnLabel);
            case UUID, TIMEUUID -> this.currentRow.getUUID(columnLabel);
            default -> null;
        };
    }

    private String getObjectAsString(final int columnIndex) throws SQLException {
        final Object o = getObject(columnIndex);
        if (o != null) {
            return String.valueOf(o);
        }
        return null;
    }

    private String getObjectAsString(final String columnLabel) throws SQLException {
        final Object o = getObject(columnLabel);
        if (o != null) {
            return String.valueOf(o);
        }
        return null;
    }

    @Override
    public int getRow() throws SQLException {
        checkNotClosed();
        return this.rowNumber;
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
    public Set<?> getSet(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        try {
            final SetType setType = (SetType) getCqlDataType(columnIndex);
            return this.currentRow.getSet(columnIndex - 1,
                Class.forName(DataTypeEnum.fromCqlTypeName(setType.getElementType().asCql(false, false)).asJavaClass()
                    .getCanonicalName()));
        } catch (final ClassNotFoundException e) {
            throw new SQLNonTransientException(e);
        }
    }

    @Override
    public Set<?> getSet(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        try {
            final SetType setType = (SetType) getCqlDataType(columnLabel);
            return this.currentRow.getSet(columnLabel,
                Class.forName(DataTypeEnum.fromCqlTypeName(setType.getElementType().asCql(false, false)).asJavaClass()
                    .getCanonicalName()));
        } catch (final ClassNotFoundException e) {
            throw new SQLNonTransientException(e);
        }
    }

    @Override
    public short getShort(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        return this.currentRow.getShort(columnIndex - 1);
    }

    @Override
    public short getShort(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        return this.currentRow.getShort(columnLabel);
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkNotClosed();
        return this.statement;
    }

    @Override
    public String getString(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        try {
            if (DataTypeEnum.fromCqlTypeName(getCqlDataType(columnIndex).asCql(false, false)).isCollection()) {
                return getObjectAsString(columnIndex);
            }
            return this.currentRow.getString(columnIndex - 1);
        } catch (final Exception e) {
            return getObjectAsString(columnIndex);
        }
    }

    @Override
    public String getString(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        try {
            if (DataTypeEnum.fromCqlTypeName(getCqlDataType(columnLabel).asCql(false, false)).isCollection()) {
                return getObjectAsString(columnLabel);
            }
            return this.currentRow.getString(columnLabel);
        } catch (final Exception e) {
            return getObjectAsString(columnLabel);
        }
    }

    @Override
    public Time getTime(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        return this.currentRow.getTime(columnIndex - 1);
    }

    @Override
    public Time getTime(final int columnIndex, final Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; it's a hint we do not need
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        return this.currentRow.getTime(columnLabel);
    }

    @Override
    public Time getTime(final String columnLabel, final Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; it's a hint we do not need
        return getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final java.util.Date date = this.currentRow.getDate(columnIndex - 1);
        if (date == null) {
            return null;
        }
        return new Timestamp(this.currentRow.getDate(columnIndex - 1).getTime());
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; it's a hint we do not need
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final java.util.Date date = this.currentRow.getDate(columnLabel);
        if (date == null) {
            return null;
        }
        return new Timestamp(this.currentRow.getDate(columnLabel).getTime());
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel, final Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; it's a hint we do not need
        return getTimestamp(columnLabel);
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getType() throws SQLException {
        checkNotClosed();
        return this.resultSetType;
    }

    @Override
    public URL getURL(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        // Handle URL data type as a String.
        final String storedUrl = this.currentRow.getString(columnIndex - 1);
        if (storedUrl == null) {
            return null;
        } else {
            try {
                return new URI(storedUrl).toURL();
            } catch (final URISyntaxException | MalformedURLException e) {
                throw new SQLException(format(MALFORMED_URL, storedUrl), e);
            }
        }
    }

    @Override
    public URL getURL(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        // Handle URL data type as a String.
        final String storedUrl = this.currentRow.getString(columnLabel);
        if (storedUrl == null) {
            return null;
        } else {
            try {
                return new URI(storedUrl).toURL();
            } catch (final URISyntaxException | MalformedURLException e) {
                throw new SQLException(format(MALFORMED_URL, storedUrl), e);
            }
        }
    }

    @Override
    public CqlVector<?> getVector(final int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public CqlVector<?> getVector(final String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // The rationale is there are no warnings to return in this implementation, but it still throws an exception
        // when called on a closed result set.
        checkNotClosed();
        return null;
    }

    private boolean hasMoreRows() {
        return this.rowsIterator != null
            && (this.rowsIterator.hasNext() || (this.rowNumber == 0 && this.currentRow != null));
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkNotClosed();
        return this.rowNumber == Integer.MAX_VALUE;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkNotClosed();
        return this.rowNumber == 0;
    }

    @Override
    public boolean isClosed() {
        return this.isClosed;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkNotClosed();
        return this.rowNumber == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkNotClosed();
        return !this.rowsIterator.hasNext();
    }

    @Override
    public synchronized boolean next() {
        if (hasMoreRows()) {
            // 'populateColumns()' is called upon init to set up the metadata fields; so skip the first call.
            if (this.rowNumber != 0) {
                populateColumns();
            }
            this.rowNumber++;
            return true;
        }
        this.rowNumber = Integer.MAX_VALUE;
        return false;
    }

    @Override
    public boolean wasNull() {
        return this.wasNull;
    }

    /**
     * Implementation class for {@link ResultSetMetaData}. The metadata returned refers to the column values, not the
     * column names.
     */
    class CResultSetMetaData implements ResultSetMetaData {
        @Override
        public String getCatalogName(final int column) throws SQLException {
            if (statement == null) {
                return null;
            }
            return statement.connection.getCatalog();
        }

        @Override
        public String getColumnClassName(final int column) throws SQLException {
            if (currentRow != null) {
                return DataTypeEnum.fromCqlTypeName(getCqlDataType(column).asCql(false, false)).asJavaClass()
                    .getCanonicalName();
            }
            if (driverResultSet != null && driverResultSet.getColumnDefinitions() != null) {
                return DataTypeEnum.fromCqlTypeName(
                        driverResultSet.getColumnDefinitions().asList().get(column - 1).getType().asCql(false, false))
                    .asJavaClass().getCanonicalName();
            }
            throw new SQLException(UNABLE_TO_RETRIEVE_METADATA);
        }

        @Override
        public int getColumnCount() {
            if (currentRow != null) {
                return currentRow.getColumnDefinitions().size();
            }
            if (driverResultSet != null && driverResultSet.getColumnDefinitions() != null) {
                return driverResultSet.getColumnDefinitions().size();
            }
            return 0;
        }

        @Override
        public String getColumnLabel(final int column) throws SQLException {
            return getColumnName(column);
        }

        @Override
        public String getColumnName(final int column) throws SQLException {
            if (currentRow != null && currentRow.getColumnDefinitions() != null) {
                return currentRow.getColumnDefinitions().getName(column - 1);
            }
            if (driverResultSet != null && driverResultSet.getColumnDefinitions() != null) {
                return driverResultSet.getColumnDefinitions().asList().get(column - 1).getName();
            }
            throw new SQLException(UNABLE_TO_RETRIEVE_METADATA);
        }

        @Override
        public int getColumnDisplaySize(final int column) {
            try {
                final AbstractJdbcType<?> jdbcEquivalentType;
                final ColumnDefinitions.Definition columnDefinition;
                if (currentRow != null && currentRow.getColumnDefinitions() != null) {
                    columnDefinition = currentRow.getColumnDefinitions().asList().get(column - 1);
                } else if (driverResultSet != null && driverResultSet.getColumnDefinitions() != null) {
                    columnDefinition = driverResultSet.getColumnDefinitions().asList().get(column - 1);
                } else {
                    return DEFAULT_PRECISION;
                }
                jdbcEquivalentType = TypesMap.getTypeForComparator(columnDefinition.getType().toString());

                int length = DEFAULT_PRECISION;
                if (jdbcEquivalentType != null) {
                    length = jdbcEquivalentType.getPrecision(null);
                }
                return length;
            } catch (final Exception e) {
                return DEFAULT_PRECISION;
            }
        }

        @Override
        public int getColumnType(final int column) throws SQLException {
            final DataType type;
            if (currentRow != null) {
                type = getCqlDataType(column);
            } else if (driverResultSet != null && driverResultSet.getColumnDefinitions() != null) {
                type = driverResultSet.getColumnDefinitions().asList().get(column - 1).getType();
            } else {
                return Types.OTHER;
            }
            return TypesMap.getTypeForComparator(type.toString()).getJdbcType();
        }

        @Override
        public String getColumnTypeName(final int column) throws SQLException {
            // Specification says "database specific type name"; for Cassandra this means the AbstractType.
            final DataType type;
            if (currentRow != null) {
                type = getCqlDataType(column);
            } else if (driverResultSet != null && driverResultSet.getColumnDefinitions() != null) {
                type = driverResultSet.getColumnDefinitions().getType(column - 1);
            } else {
                return StringUtils.EMPTY;
            }
            return type.toString();
        }

        @Override
        public int getPrecision(final int column) {
            return Math.max(getColumnDisplaySize(column), 0);
        }

        @Override
        public int getScale(final int column) {
            try {
                final AbstractJdbcType<?> jdbcEquivalentType;
                final ColumnDefinitions.Definition columnDefinition;
                if (currentRow != null && currentRow.getColumnDefinitions() != null) {
                    columnDefinition = currentRow.getColumnDefinitions().asList().get(column - 1);
                } else if (driverResultSet != null && driverResultSet.getColumnDefinitions() != null) {
                    columnDefinition = driverResultSet.getColumnDefinitions().asList().get(column - 1);
                } else {
                    return DEFAULT_SCALE;
                }
                jdbcEquivalentType = TypesMap.getTypeForComparator(columnDefinition.getType().toString());

                int scale = DEFAULT_SCALE;
                if (jdbcEquivalentType != null) {
                    scale = jdbcEquivalentType.getScale(null);
                }
                return scale;
            } catch (final Exception e) {
                return DEFAULT_SCALE;
            }
        }

        @Override
        public String getSchemaName(final int column) throws SQLException {
            if (statement == null) {
                return null;
            }
            return statement.connection.getSchema();
        }

        @Override
        public String getTableName(final int column) {
            final String tableName;
            if (currentRow != null && currentRow.getColumnDefinitions() != null) {
                tableName = currentRow.getColumnDefinitions().getTable(column - 1);
            } else if (driverResultSet != null && driverResultSet.getColumnDefinitions() != null) {
                tableName = driverResultSet.getColumnDefinitions().getTable(column - 1);
            } else {
                return StringUtils.EMPTY;
            }
            return tableName;
        }

        @Override
        public boolean isAutoIncrement(final int column) {
            return false;
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
        public boolean isDefinitelyWritable(final int column) {
            return isWritable(column);
        }

        @Override
        public int isNullable(final int column) {
            // Note: absence is the equivalent of null in Cassandra
            return ResultSetMetaData.columnNullable;
        }

        @Override
        public boolean isReadOnly(final int column) {
            return column == 0;
        }

        /**
         * Indicates whether the designated column can be used in a where clause.
         * <p>
         *    Using Cassandra database, we consider that only the columns in a primary key (partitioning keys and
         *    clustering columns) or in an index are searchable.<br>
         *    See: <a href="https://cassandra.apache.org/doc/latest/cassandra/cql/dml.html#where-clause">
         *        WHERE clause in CQL SELECT statements</a>
         * </p>
         *
         * @param column The column index (the first column is 1, the second is 2, ...).
         * @return {@code true} if so, {@code false} otherwise.
         * @throws SQLException if a database access error occurs.
         */
        @Override
        public boolean isSearchable(final int column) throws SQLException {
            if (statement == null) {
                return false;
            }
            final String columnName = getColumnName(column);
            final String schemaName = getSchemaName(column);
            final String tableName = getTableName(column);
            // If the schema or table name is not defined, always returns false since we cannot determine if the column
            // is searchable in this context.
            if (StringUtils.isEmpty(schemaName) || StringUtils.isEmpty(tableName)) {
                return false;
            }
            final AtomicBoolean searchable = new AtomicBoolean(false);
            statement.connection.getSession().getMetadata().getKeyspace(schemaName)
                .flatMap(ksMetadata -> ksMetadata.getTable(tableName))
                .ifPresent(tableMetadata -> {
                    boolean result;
                    // Check first if the column is a clustering column or in a partitioning key.
                    result = tableMetadata.getPrimaryKey().stream()
                        .anyMatch(columnMetadata -> columnMetadata.getName().asInternal().equals(columnName));
                    // If not, check if the column is used in an index.
                    if (!result) {
                        result = tableMetadata.getIndexes().values().stream()
                            .anyMatch(indexMetadata -> indexMetadata.getTarget().contains(columnName));
                    }
                    searchable.set(result);
                });
            return searchable.get();
        }

        @Override
        public boolean isSigned(final int column) throws SQLException {
            final DataType type;
            if (currentRow != null) {
                type = getCqlDataType(column);
            } else if (driverResultSet != null && driverResultSet.getColumnDefinitions() != null) {
                type = driverResultSet.getColumnDefinitions().asList().get(column - 1).getType();
            } else {
                return false;
            }
            return TypesMap.getTypeForComparator(type.toString()).isSigned();
        }

        @Override
        public boolean isWritable(final int column) {
            return column > 0;
        }

        @Override
        public boolean isWrapperFor(final Class<?> iface) throws SQLException {
            return iface != null && iface.isAssignableFrom(this.getClass());
        }

        @Override
        public <T> T unwrap(final Class<T> iface) throws SQLException {
            if (isWrapperFor(iface)) {
                return iface.cast(this);
            } else {
                throw new SQLException(format(NO_INTERFACE, iface.getSimpleName()));
            }
        }
    }

}
