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
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.ing.data.cassandra.jdbc.types.AbstractJdbcType;
import com.ing.data.cassandra.jdbc.types.DataTypeEnum;
import com.ing.data.cassandra.jdbc.types.TypesMap;
import com.ing.data.cassandra.jdbc.utils.ArrayImpl;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
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
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ing.data.cassandra.jdbc.types.AbstractJdbcType.DEFAULT_PRECISION;
import static com.ing.data.cassandra.jdbc.types.AbstractJdbcType.DEFAULT_SCALE;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.fromCqlTypeName;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.fromDataType;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_FETCH_DIR;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.BAD_FETCH_SIZE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.FORWARD_ONLY;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.ILLEGAL_FETCH_DIRECTION_FOR_FORWARD_ONLY;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.MALFORMED_URL;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.MUST_BE_POSITIVE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NO_INTERFACE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.UNABLE_TO_READ_VALUE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.UNSUPPORTED_JSON_TYPE_CONVERSION;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.UNSUPPORTED_TYPE_CONVERSION;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.VALID_LABELS;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.VECTOR_ELEMENTS_NOT_NUMBERS;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.WAS_CLOSED_RS;
import static com.ing.data.cassandra.jdbc.utils.JsonUtil.getObjectMapper;

/**
 * Cassandra result set: implementation class for {@link java.sql.ResultSet}.
 * <p>
 * It also implements {@link CassandraResultSetExtras} and {@link CassandraResultSetJsonSupport} interfaces
 * providing extra methods not defined in JDBC API to better handle some CQL data types and ease usage of JSON
 * features {@code SELECT JSON} and {@code toJson()} provided by Cassandra.
 * </p>
 * The supported data types in CQL are:
 * <table border="1">
 *     <tr><th>CQL Type </th><th>Java type          </th><th>Description</th></tr>
 *     <tr><td>ascii    </td><td>{@link String}     </td><td>US-ASCII character string</td></tr>
 *     <tr><td>bigint   </td><td>{@link Long}       </td><td>64-bit signed long</td></tr>
 *     <tr><td>blob     </td><td>{@link ByteBuffer} </td><td>Arbitrary bytes (no validation)</td></tr>
 *     <tr><td>boolean  </td><td>{@link Boolean}    </td><td>Boolean value: true or false</td></tr>
 *     <tr><td>counter  </td><td>{@link Long}       </td><td>Counter column (64-bit long)</td></tr>
 *     <tr><td>date     </td><td>{@link Date}       </td><td>A date with no corresponding time value; encoded date
 *     as a 32-bit integer representing days since epoch (January 1, 1970)</td></tr>
 *     <tr><td>decimal  </td><td>{@link BigDecimal} </td><td>Variable-precision decimal</td></tr>
 *     <tr><td>double   </td><td>{@link Double}     </td><td>64-bit IEEE-754 floating point</td></tr>
 *     <tr><td>duration </td><td>{@link CqlDuration}</td><td>A duration with nanosecond precision</td></tr>
 *     <tr><td>float    </td><td>{@link Float}      </td><td>32-bit IEEE-754 floating point</td></tr>
 *     <tr><td>inet     </td><td>{@link InetAddress}</td><td>IP address string in IPv4 or IPv6 format</td></tr>
 *     <tr><td>int      </td><td>{@link Integer}    </td><td>32-bit signed integer</td></tr>
 *     <tr><td>list     </td><td>{@link List}       </td><td>A collection of one or more ordered elements:
 *     <code>[literal, literal, literal]</code></td></tr>
 *     <tr><td>map      </td><td>{@link Map}        </td><td>A JSON-style array of literals:
 *     <code>{ literal : literal, literal : literal ... }</code></td></tr>
 *     <tr><td>set      </td><td>{@link Set}        </td><td>A collection of one or more elements:
 *     <code>{ literal, literal, literal }</code></td></tr>
 *     <tr><td>smallint </td><td>{@link Short}      </td><td>16-bit signed integer</td></tr>
 *     <tr><td>text     </td><td>{@link String}     </td><td>UTF-8 encoded string</td></tr>
 *     <tr><td>time     </td><td>{@link Time}       </td><td>A value encoded as a 64-bit signed integer
 *     representing the number of nanoseconds since midnight</td></tr>
 *     <tr><td>timestamp</td><td>{@link Timestamp}  </td><td>Date and time with millisecond precision, encoded as
 *     8 bytes since epoch</td></tr>
 *     <tr><td>timeuuid </td><td>{@link UUID}       </td><td>Version 1 UUID only</td></tr>
 *     <tr><td>tinyint  </td><td>{@link Byte}       </td><td>8-bits signed integer</td></tr>
 *     <tr><td>tuple    </td><td>{@link TupleValue} </td><td>A group of 2-3 fields</td></tr>
 *     <tr><td>udt      </td><td>{@link UdtValue}   </td><td>A set of data fields where each field is named and
 *     typed</td></tr>
 *     <tr><td>uuid     </td><td>{@link UUID}       </td><td>A UUID in standard UUID format</td></tr>
 *     <tr><td>varchar  </td><td>{@link String}     </td><td>UTF-8 encoded string</td></tr>
 *     <tr><td>varint   </td><td>{@link BigInteger} </td><td>Arbitrary-precision integer</td></tr>
 *     <tr><td>vector   </td><td>{@link CqlVector}  </td><td>A n-dimensional vector</td></tr>
 * </table>
 * See: <a href="https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/cql_data_types_c.html">
 *     CQL data types reference</a> and
 * <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/temporal_types/">
 *     CQL temporal types reference</a>.
 *
 * @see ResultSet
 */
public class CassandraResultSet extends AbstractResultSet
    implements CassandraResultSetExtras, CassandraResultSetJsonSupport {

    /**
     * An empty Cassandra result set. It can be used to provide default implementations to methods returning
     * {@link ResultSet} objects.
     */
    public static final CassandraResultSet EMPTY_RESULT_SET = new CassandraResultSet();
    /**
     * Default result set type for Cassandra implementation: {@link #TYPE_FORWARD_ONLY}.
     */
    public static final int DEFAULT_TYPE = TYPE_FORWARD_ONLY;
    /**
     * Default result set concurrency for Cassandra implementation: {@link #CONCUR_READ_ONLY}.
     */
    public static final int DEFAULT_CONCURRENCY = CONCUR_READ_ONLY;
    /**
     * Default result set holdability for Cassandra implementation: {@link #HOLD_CURSORS_OVER_COMMIT}.
     */
    public static final int DEFAULT_HOLDABILITY = HOLD_CURSORS_OVER_COMMIT;

    private static final Logger LOG = LoggerFactory.getLogger(CassandraResultSet.class);

    int rowNumber = 0;
    // Metadata of this result set.
    private final CResultSetMetaData metadata;
    private final CassandraStatement statement;
    private Row currentRow;
    private Iterator<Row> rowsIterator;
    private int resultSetType;
    private int fetchDirection;
    private int fetchSize;
    private boolean wasNull;
    private boolean isClosed;
    // Result set from the Cassandra driver.
    private ResultSet driverResultSet;

    /**
     * No argument constructor.
     */
    CassandraResultSet() {
        this.metadata = new CResultSetMetaData();
        this.statement = null;
        this.isClosed = false;
    }

    /**
     * Constructor. It instantiates a new Cassandra result set from a {@link ResultSet}.
     *
     * @param statement The statement.
     * @param resultSet The result set from the Cassandra driver.
     * @throws SQLException if a database access error occurs or this constructor is called with a closed
     *                      {@link Statement}.
     */
    CassandraResultSet(final CassandraStatement statement, final ResultSet resultSet) throws SQLException {
        this.metadata = new CResultSetMetaData();
        this.statement = statement;
        this.resultSetType = statement.getResultSetType();
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();
        this.driverResultSet = resultSet;
        this.rowsIterator = resultSet.iterator();
        this.isClosed = false;

        // Initialize the column values from the first row.
        if (hasMoreRows()) {
            populateColumns();
        }
    }

    /**
     * Constructor. It instantiates a new Cassandra result set from a list of {@link ResultSet}.
     *
     * @param statement  The statement.
     * @param resultSets The list of result sets from the Cassandra driver.
     * @throws SQLException if a database access error occurs or this constructor is called with a closed
     *                      {@link Statement}.
     */
    @SuppressWarnings("unchecked")
    CassandraResultSet(final CassandraStatement statement, final ArrayList<ResultSet> resultSets) throws SQLException {
        this.metadata = new CResultSetMetaData();
        this.statement = statement;
        this.resultSetType = statement.getResultSetType();
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();
        this.isClosed = false;

        // We have several result sets, but we will use only the first one for metadata needs.
        this.driverResultSet = resultSets.get(0);

        // Now, we concatenate iterators of the different result sets into a single one.
        // This may lead to StackOverflowException when there are too many result sets.
        final Iterator<Row>[] resultSetsIterators = new Iterator[resultSets.size()];
        resultSetsIterators[0] = this.driverResultSet.iterator();
        for (int i = 1; i < resultSets.size(); i++) {
            resultSetsIterators[i] = resultSets.get(i).iterator();
        }
        this.rowsIterator = IteratorUtils.chainedIterator(resultSetsIterators);

        // Initialize the column values from the first row.
        if (hasMoreRows()) {
            populateColumns();
        }
    }

    private void populateColumns() {
        this.currentRow = this.rowsIterator.next();
    }

    @Override
    DataType getCqlDataType(final int columnIndex) {
        if (this.currentRow != null) {
            return this.currentRow.getColumnDefinitions().get(columnIndex - 1).getType();
        }
        return this.driverResultSet.getColumnDefinitions().get(columnIndex - 1).getType();
    }

    @Override
    DataType getCqlDataType(final String columnLabel) {
        if (this.currentRow != null) {
            return this.currentRow.getColumnDefinitions().get(columnLabel).getType();
        }
        return this.driverResultSet.getColumnDefinitions().get(columnLabel).getType();
    }

    @Override
    public void afterLast() throws SQLException {
        if (this.resultSetType == TYPE_FORWARD_ONLY) {
            throw new SQLNonTransientException(FORWARD_ONLY);
        }
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (this.resultSetType == TYPE_FORWARD_ONLY) {
            throw new SQLNonTransientException(FORWARD_ONLY);
        }
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    private void checkIndex(final int index) throws SQLException {
        if (this.currentRow != null) {
            if (index < 1 || index > this.currentRow.getColumnDefinitions().size()) {
                throw new SQLSyntaxErrorException(String.format(MUST_BE_POSITIVE, index) + StringUtils.SPACE
                    + this.currentRow.getColumnDefinitions().size());
            }
            this.wasNull = this.currentRow.isNull(index - 1);
        } else if (this.driverResultSet != null) {
            if (index < 1 || index > this.driverResultSet.getColumnDefinitions().size()) {
                throw new SQLSyntaxErrorException(String.format(MUST_BE_POSITIVE, index) + StringUtils.SPACE
                    + this.driverResultSet.getColumnDefinitions().size());
            }
        }

    }

    private void checkName(final String name) throws SQLException {
        if (this.currentRow != null) {
            if (!this.currentRow.getColumnDefinitions().contains(name)) {
                throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
            }
            this.wasNull = this.currentRow.isNull(name);
        } else if (this.driverResultSet != null) {
            if (!this.driverResultSet.getColumnDefinitions().contains(name)) {
                throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
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
            return this.currentRow.getColumnDefinitions().firstIndexOf(columnLabel) + 1;
        } else if (this.driverResultSet != null) {
            return this.driverResultSet.getColumnDefinitions().firstIndexOf(columnLabel) + 1;
        }
        throw new SQLSyntaxErrorException(String.format(VALID_LABELS, columnLabel));
    }

    @Override
    public InputStream getAsciiStream(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final String s = this.currentRow.getString(columnIndex - 1);
        if (s != null) {
            return new ByteArrayInputStream(s.getBytes(StandardCharsets.US_ASCII));
        } else {
            return null;
        }
    }

    @Override
    public Array getArray(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        Object o = currentRow.getObject(columnIndex - 1);
        return o instanceof List ? toArray((List<?>) o) : null;
    }

    @Override
    public Array getArray(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        Object o = currentRow.getObject(columnLabel);
        return o instanceof List ? toArray((List<?>) o) : null;
    }

    private Array toArray(final List<?> list) {
        Object[] array = new Object[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return new ArrayImpl(array);
    }

    @Override
    public InputStream getAsciiStream(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final String s = this.currentRow.getString(columnLabel);
        if (s != null) {
            return new ByteArrayInputStream(s.getBytes(StandardCharsets.US_ASCII));
        } else {
            return null;
        }
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        return this.currentRow.getBigDecimal(columnIndex - 1);
    }

    /**
     * @deprecated use {@link #getBigDecimal(int)}.
     */
    @Override
    @Deprecated
    public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
        checkIndex(columnIndex);
        final BigDecimal decimalValue = this.currentRow.getBigDecimal(columnIndex - 1);
        if (decimalValue == null) {
            return null;
        } else {
            return decimalValue.setScale(scale, RoundingMode.HALF_UP);
        }
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        return this.currentRow.getBigDecimal(columnLabel);
    }

    /**
     * @deprecated use {@link #getBigDecimal(String)}.
     */
    @Override
    @Deprecated
    public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
        checkName(columnLabel);
        final BigDecimal decimalValue = this.currentRow.getBigDecimal(columnLabel);
        if (decimalValue == null) {
            return null;
        } else {
            return decimalValue.setScale(scale, RoundingMode.HALF_UP);
        }
    }

    @Override
    public BigInteger getBigInteger(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        return this.currentRow.getBigInteger(columnIndex - 1);
    }

    @Override
    public BigInteger getBigInteger(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        return this.currentRow.getBigInteger(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final ByteBuffer byteBuffer = this.currentRow.getByteBuffer(columnIndex - 1);
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
        final ByteBuffer byteBuffer = this.currentRow.getByteBuffer(columnLabel);
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
        final ByteBuffer byteBuffer = this.currentRow.getByteBuffer(columnIndex - 1);
        if (byteBuffer != null) {
            return new javax.sql.rowset.serial.SerialBlob(byteBuffer.array());
        } else {
            return null;
        }
    }

    @Override
    public Blob getBlob(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final ByteBuffer byteBuffer = this.currentRow.getByteBuffer(columnLabel);
        if (byteBuffer != null) {
            return new javax.sql.rowset.serial.SerialBlob(byteBuffer.array());
        } else {
            return null;
        }
    }

    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        return this.currentRow.getBoolean(columnIndex - 1);
    }

    @Override
    public boolean getBoolean(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        return this.currentRow.getBoolean(columnLabel);
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
        final ByteBuffer byteBuffer = this.currentRow.getByteBuffer(columnIndex - 1);
        if (byteBuffer != null) {
            return byteBuffer.array();
        }
        return null;
    }

    @Override
    public byte[] getBytes(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final ByteBuffer byteBuffer = this.currentRow.getByteBuffer(columnLabel);
        if (byteBuffer != null) {
            return byteBuffer.array();
        }
        return null;
    }

    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final byte[] byteArray = this.getBytes(columnIndex);
        if (byteArray != null) {
            final InputStream inputStream = new ByteArrayInputStream(byteArray);
            try {
                return new CharArrayReader(IOUtils.toCharArray(inputStream, StandardCharsets.UTF_8));
            } catch (final IOException e) {
                throw new SQLException(String.format(UNABLE_TO_READ_VALUE, Reader.class.getSimpleName()), e);
            }
        } else {
            return null;
        }
    }

    @Override
    public Reader getCharacterStream(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final byte[] byteArray = this.getBytes(columnLabel);
        if (byteArray != null) {
            final InputStream inputStream = new ByteArrayInputStream(byteArray);
            try {
                return new CharArrayReader(IOUtils.toCharArray(inputStream, StandardCharsets.UTF_8));
            } catch (final IOException e) {
                throw new SQLException(String.format(UNABLE_TO_READ_VALUE, Reader.class.getSimpleName()), e);
            }
        } else {
            return null;
        }
    }

    @Override
    public Clob getClob(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final byte[] byteArray = getBytes(columnIndex);
        if (byteArray != null) {
            final InputStream inputStream = new ByteArrayInputStream(byteArray);
            try {
                return new javax.sql.rowset.serial.SerialClob(IOUtils.toCharArray(inputStream, StandardCharsets.UTF_8));
            } catch (final IOException e) {
                throw new SQLException(String.format(UNABLE_TO_READ_VALUE, Clob.class.getSimpleName()), e);
            }
        } else {
            return null;
        }
    }

    @Override
    public Clob getClob(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final byte[] byteArray = getBytes(columnLabel);
        if (byteArray != null) {
            final InputStream inputStream = new ByteArrayInputStream(byteArray);
            try {
                return new javax.sql.rowset.serial.SerialClob(IOUtils.toCharArray(inputStream, StandardCharsets.UTF_8));
            } catch (final IOException e) {
                throw new SQLException(String.format(UNABLE_TO_READ_VALUE, Clob.class.getSimpleName()), e);
            }
        } else {
            return null;
        }
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkNotClosed();
        return this.statement.getResultSetConcurrency();
    }

    @Override
    public Date getDate(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final LocalDate localDate = this.currentRow.getLocalDate(columnIndex - 1);
        if (localDate == null) {
            return null;
        } else {
            return java.sql.Date.valueOf(localDate);
        }
    }

    @Override
    public Date getDate(final int columnIndex, final Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; it's a hint we do not need
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final LocalDate localDate = this.currentRow.getLocalDate(columnLabel);
        if (localDate == null) {
            return null;
        } else {
            return java.sql.Date.valueOf(localDate);
        }
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
        return this.currentRow.getCqlDuration(columnIndex - 1);
    }

    @Override
    public CqlDuration getDuration(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        return this.currentRow.getCqlDuration(columnLabel);
    }

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
                throw new SQLSyntaxErrorException(String.format(ILLEGAL_FETCH_DIRECTION_FOR_FORWARD_ONLY, direction));
            }
            this.fetchDirection = direction;
        }
        throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
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
            throw new SQLException(String.format(BAD_FETCH_SIZE, size));
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

        if (fromCqlTypeName(cqlDataType.asCql(false, false)).isCollection()) {
            try {
                final ListType listType = (ListType) cqlDataType;
                final Class<?> itemsClass = Class.forName(fromDataType(listType.getElementType())
                    .asJavaClass().getCanonicalName());
                final List<?> resultList = this.currentRow.getList(columnIndex - 1, itemsClass);
                if (resultList == null) {
                    return null;
                }
                return new ArrayList<>(resultList);
            } catch (final ClassNotFoundException e) {
                LOG.warn("Error while executing getList()", e);
            }
        }
        return this.currentRow.getList(columnIndex - 1, String.class);
    }

    @Override
    public List<?> getList(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        if (fromCqlTypeName(getCqlDataType(columnLabel).asCql(false, false)).isCollection()) {
            try {
                final ListType listType = (ListType) getCqlDataType(columnLabel);
                final Class<?> itemsClass = Class.forName(fromDataType(listType.getElementType())
                    .asJavaClass().getCanonicalName());
                final List<?> resultList = this.currentRow.getList(columnLabel, itemsClass);
                if (resultList == null) {
                    return null;
                }
                return new ArrayList<>(resultList);
            } catch (final ClassNotFoundException e) {
                LOG.warn("Error while executing getList()", e);
            }
        }
        return this.currentRow.getList(columnLabel, String.class);
    }

    /**
     * Retrieves the value of the designated column in the current row of this {@code ResultSet} object as a
     * {@link LocalDate}.
     *
     * @param columnIndex The column index (the first column is 1).
     * @return The column value. If the value is SQL {@code NULL}, it should return {@code null}.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs or this method is called
     *                      on a closed result set.
     */
    public LocalDate getLocalDate(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        return this.currentRow.getLocalDate(columnIndex - 1);
    }

    @Override
    public long getLong(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        if (isCqlType(columnIndex, DataTypeEnum.INT)) {
            return this.currentRow.getInt(columnIndex - 1);
        } else if (isCqlType(columnIndex, DataTypeEnum.VARINT)) {
            final BigInteger bigintValue = currentRow.getBigInteger(columnIndex - 1);
            if (bigintValue != null) {
                return bigintValue.longValue();
            } else {
                return 0;
            }
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
            final BigInteger bigintValue = currentRow.getBigInteger(columnLabel);
            if (bigintValue != null) {
                return bigintValue.longValue();
            } else {
                return 0;
            }
        } else {
            return this.currentRow.getLong(columnLabel);
        }
    }

    @Override
    public Map<?, ?> getMap(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);

        final DefaultMapType mapType = (DefaultMapType) getCqlDataType(columnIndex);
        final Class<?> keysClass = fromDataType(mapType.getKeyType()).javaType;
        final Class<?> valuesClass = fromDataType(mapType.getValueType()).javaType;

        return this.currentRow.getMap(columnIndex - 1, keysClass, valuesClass);
    }

    @Override
    public Map<?, ?> getMap(final String columnLabel) throws SQLException {
        checkName(columnLabel);

        final DefaultMapType mapType = (DefaultMapType) getCqlDataType(columnLabel);
        final Class<?> keysClass = fromDataType(mapType.getKeyType()).javaType;
        final Class<?> valuesClass = fromDataType(mapType.getValueType()).javaType;

        return this.currentRow.getMap(columnLabel, keysClass, valuesClass);
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return this.metadata;
    }

    @Override
    public NClob getNClob(final int columnIndex) throws SQLException {
        return (NClob) getClob(columnIndex);
    }

    @Override
    public NClob getNClob(final String columnLabel) throws SQLException {
        return (NClob) getClob(columnLabel);
    }

    @Override
    public Object getObject(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final DataType cqlDataType = getCqlDataType(columnIndex);
        final DataTypeEnum dataType = fromDataType(cqlDataType);

        if (currentRow.isNull(columnIndex - 1)) {
            return null;
        }

        // Collections: sets, lists, vectors & maps
        if (dataType.isCollection()) {
            // Sets
            if (isCqlType(columnIndex, DataTypeEnum.SET)) {
                final SetType setType = (SetType) cqlDataType;
                final DataType elementsType = setType.getElementType();
                final Set<?> resultSet;

                if (elementsType instanceof UserDefinedType) {
                    resultSet = this.currentRow.getSet(columnIndex - 1,
                        TypesMap.getTypeForComparator(DataTypeEnum.UDT.asLowercaseCql()).getType());
                } else if (elementsType instanceof TupleType) {
                    resultSet = this.currentRow.getSet(columnIndex - 1,
                        TypesMap.getTypeForComparator(DataTypeEnum.TUPLE.asLowercaseCql()).getType());
                } else {
                    resultSet = this.currentRow.getSet(columnIndex - 1,
                        TypesMap.getTypeForComparator(elementsType.asCql(false, false)).getType());
                }
                if (resultSet == null) {
                    return null;
                }
                return new LinkedHashSet<>(resultSet);
            }

            // Lists
            if (isCqlType(columnIndex, DataTypeEnum.LIST)) {
                final ListType listType = (ListType) cqlDataType;
                final DataType elementsType = listType.getElementType();
                final List<?> resultList;

                if (elementsType instanceof TupleType) {
                    resultList = this.currentRow.getList(columnIndex - 1,
                        TypesMap.getTypeForComparator(DataTypeEnum.TUPLE.asLowercaseCql()).getType());
                } else {
                    resultList = this.currentRow.getList(columnIndex - 1,
                        TypesMap.getTypeForComparator(elementsType.asCql(false, false)).getType());
                }
                if (resultList == null) {
                    return null;
                }
                return new ArrayList<>(resultList);
            }

            // Vectors
            if (isCqlType(columnIndex, DataTypeEnum.VECTOR)) {
                return getVector(columnIndex);
            }

            // Maps
            if (isCqlType(columnIndex, DataTypeEnum.MAP)) {
                final MapType mapType = (MapType) cqlDataType;
                final DataType keyType = mapType.getKeyType();
                final DataType valueType = mapType.getValueType();

                Class<?> keyClass = TypesMap.getTypeForComparator(keyType.asCql(false, false)).getType();
                if (keyType instanceof UserDefinedType) {
                    keyClass = TypesMap.getTypeForComparator(DataTypeEnum.UDT.asLowercaseCql()).getType();
                } else if (keyType instanceof TupleType) {
                    keyClass = TypesMap.getTypeForComparator(DataTypeEnum.TUPLE.asLowercaseCql()).getType();
                }

                Class<?> valueClass = TypesMap.getTypeForComparator(valueType.asCql(false, false)).getType();
                if (valueType instanceof UserDefinedType) {
                    valueClass = TypesMap.getTypeForComparator(DataTypeEnum.UDT.asLowercaseCql()).getType();
                } else if (valueType instanceof TupleType) {
                    valueClass = TypesMap.getTypeForComparator(DataTypeEnum.TUPLE.asLowercaseCql()).getType();
                }

                final Map<?, ?> resultMap = this.currentRow.getMap(columnIndex - 1, keyClass, valueClass);
                if (resultMap == null) {
                    return null;
                }
                return new HashMap<>(resultMap);
            }
        } else {
            // Other types.
            switch (dataType) {
                case VARCHAR:
                case ASCII:
                case TEXT:
                    return this.currentRow.getString(columnIndex - 1);
                case VARINT:
                    return this.currentRow.getBigInteger(columnIndex - 1);
                case INT:
                    return this.currentRow.getInt(columnIndex - 1);
                case SMALLINT:
                    return this.currentRow.getShort(columnIndex - 1);
                case TINYINT:
                    return this.currentRow.getByte(columnIndex - 1);
                case BIGINT:
                case COUNTER:
                    return this.currentRow.getLong(columnIndex - 1);
                case BLOB:
                    return this.currentRow.getByteBuffer(columnIndex - 1);
                case BOOLEAN:
                    return this.currentRow.getBoolean(columnIndex - 1);
                case DECIMAL:
                    return this.currentRow.getBigDecimal(columnIndex - 1);
                case DOUBLE:
                    return this.currentRow.getDouble(columnIndex - 1);
                case FLOAT:
                    return this.currentRow.getFloat(columnIndex - 1);
                case INET:
                    return this.currentRow.getInetAddress(columnIndex - 1);
                case DATE:
                    return getDate(columnIndex);
                case TIME:
                    return getTime(columnIndex);
                case TIMESTAMP:
                    return getTimestamp(columnIndex);
                case DURATION:
                    return this.currentRow.getCqlDuration(columnIndex - 1);
                case UUID:
                case TIMEUUID:
                    return this.currentRow.getUuid(columnIndex - 1);
                case UDT:
                    return this.currentRow.getUdtValue(columnIndex - 1);
                case TUPLE:
                    return this.currentRow.getTupleValue(columnIndex - 1);
            }
        }

        return null;
    }

    @Override
    public Object getObject(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final DataType cqlDataType = getCqlDataType(columnLabel);
        final DataTypeEnum dataType = fromDataType(cqlDataType);

        if (currentRow.isNull(columnLabel)) {
            return null;
        }

        // Collections: sets, lists, vectors & maps
        if (dataType.isCollection()) {
            // Sets
            if (isCqlType(columnLabel, DataTypeEnum.SET)) {
                final SetType setType = (SetType) cqlDataType;
                final DataType elementsType = setType.getElementType();
                final Set<?> resultSet;

                if (elementsType instanceof UserDefinedType) {
                    resultSet = this.currentRow.getSet(columnLabel,
                        TypesMap.getTypeForComparator(DataTypeEnum.UDT.asLowercaseCql()).getType());
                } else if (elementsType instanceof TupleType) {
                    resultSet = this.currentRow.getSet(columnLabel,
                        TypesMap.getTypeForComparator(DataTypeEnum.TUPLE.asLowercaseCql()).getType());
                } else {
                    resultSet = this.currentRow.getSet(columnLabel,
                        TypesMap.getTypeForComparator(elementsType.asCql(false, false)).getType());
                }
                if (resultSet == null) {
                    return null;
                }
                return new LinkedHashSet<>(resultSet);
            }

            // Lists
            if (isCqlType(columnLabel, DataTypeEnum.LIST)) {
                final ListType listType = (ListType) cqlDataType;
                final DataType elementsType = listType.getElementType();
                final List<?> resultList;

                if (elementsType instanceof TupleType) {
                    resultList = this.currentRow.getList(columnLabel,
                        TypesMap.getTypeForComparator(DataTypeEnum.TUPLE.asLowercaseCql()).getType());
                } else {
                    resultList = this.currentRow.getList(columnLabel,
                        TypesMap.getTypeForComparator(elementsType.asCql(false, false)).getType());
                }
                if (resultList == null) {
                    return null;
                }
                return new ArrayList<>(resultList);
            }

            // Vectors
            if (isCqlType(columnLabel, DataTypeEnum.VECTOR)) {
                return getVector(columnLabel);
            }

            // Maps
            if (isCqlType(columnLabel, DataTypeEnum.MAP)) {
                final MapType mapType = (MapType) cqlDataType;
                final DataType keyType = mapType.getKeyType();
                final DataType valueType = mapType.getValueType();

                Class<?> keyClass = TypesMap.getTypeForComparator(keyType.asCql(false, false)).getType();
                if (keyType instanceof UserDefinedType) {
                    keyClass = TypesMap.getTypeForComparator(DataTypeEnum.UDT.asLowercaseCql()).getType();
                } else if (keyType instanceof TupleType) {
                    keyClass = TypesMap.getTypeForComparator(DataTypeEnum.TUPLE.asLowercaseCql()).getType();
                }

                Class<?> valueClass = TypesMap.getTypeForComparator(valueType.asCql(false, false)).getType();
                if (valueType instanceof UserDefinedType) {
                    valueClass = TypesMap.getTypeForComparator(DataTypeEnum.UDT.asLowercaseCql()).getType();
                } else if (valueType instanceof TupleType) {
                    valueClass = TypesMap.getTypeForComparator(DataTypeEnum.TUPLE.asLowercaseCql()).getType();
                }

                final Map<?, ?> resultMap = this.currentRow.getMap(columnLabel, keyClass, valueClass);
                if (resultMap == null) {
                    return null;
                }
                return new HashMap<>(resultMap);
            }
        } else {
            // Other types.
            switch (dataType) {
                case VARCHAR:
                case ASCII:
                case TEXT:
                    return this.currentRow.getString(columnLabel);
                case VARINT:
                    return this.currentRow.getBigInteger(columnLabel);
                case INT:
                    return this.currentRow.getInt(columnLabel);
                case SMALLINT:
                    return this.currentRow.getShort(columnLabel);
                case TINYINT:
                    return this.currentRow.getByte(columnLabel);
                case BIGINT:
                case COUNTER:
                    return this.currentRow.getLong(columnLabel);
                case BLOB:
                    return this.currentRow.getByteBuffer(columnLabel);
                case BOOLEAN:
                    return this.currentRow.getBoolean(columnLabel);
                case DECIMAL:
                    return this.currentRow.getBigDecimal(columnLabel);
                case DOUBLE:
                    return this.currentRow.getDouble(columnLabel);
                case FLOAT:
                    return this.currentRow.getFloat(columnLabel);
                case INET:
                    return this.currentRow.getInetAddress(columnLabel);
                case DATE:
                    return getDate(columnLabel);
                case TIME:
                    return getTime(columnLabel);
                case TIMESTAMP:
                    return getTimestamp(columnLabel);
                case DURATION:
                    return this.currentRow.getCqlDuration(columnLabel);
                case UUID:
                case TIMEUUID:
                    return this.currentRow.getUuid(columnLabel);
                case UDT:
                    return this.currentRow.getUdtValue(columnLabel);
                case TUPLE:
                    return this.currentRow.getTupleValue(columnLabel);
            }
        }

        return null;
    }

    @Override
    public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
        final int index = findColumn(columnLabel);
        return getObject(index, type);
    }

    @Override
    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
        final Object returnValue;
        if (type == String.class) {
            returnValue = getString(columnIndex);
        } else if (type == Byte.class) {
            final byte byteValue = getByte(columnIndex);
            returnValue = valueOrNull(byteValue);
        } else if (type == Short.class) {
            final short shortValue = getShort(columnIndex);
            returnValue = valueOrNull(shortValue);
        } else if (type == Integer.class) {
            final int intValue = getInt(columnIndex);
            returnValue = valueOrNull(intValue);
        } else if (type == Long.class) {
            final long longValue = getLong(columnIndex);
            returnValue = valueOrNull(longValue);
        } else if (type == BigDecimal.class) {
            returnValue = getBigDecimal(columnIndex);
        } else if (type == Boolean.class) {
            final boolean booleanValue = getBoolean(columnIndex);
            returnValue = valueOrNull(booleanValue);
        } else if (type == java.sql.Date.class) {
            returnValue = getDate(columnIndex);
        } else if (type == Time.class) {
            returnValue = getTime(columnIndex);
        } else if (type == Timestamp.class) {
            returnValue = getTimestamp(columnIndex);
        } else if (type == LocalDate.class) {
            returnValue = getLocalDate(columnIndex);
        } else if (type == LocalDateTime.class || type == LocalTime.class || type == Calendar.class) {
            final Timestamp timestamp = getTimestamp(columnIndex, Calendar.getInstance());
            if (timestamp == null) {
                returnValue = null;
            } else {
                final LocalDateTime ldt = LocalDateTime.ofInstant(timestamp.toInstant(), ZoneId.of("UTC"));
                if (type == java.time.LocalDateTime.class) {
                    returnValue = ldt;
                } else if (type == java.time.LocalTime.class) {
                    returnValue = ldt.toLocalTime();
                } else {
                    returnValue = new Calendar.Builder().setInstant(ldt.toEpochSecond(ZoneOffset.UTC)).build();
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
            returnValue = valueOrNull(UUID.fromString(uuidString));
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
            returnValue = valueOrNull(floatValue);
        } else if (type == Double.class) {
            final double doubleValue = getDouble(columnIndex);
            returnValue = valueOrNull(doubleValue);
        } else if (type == CqlDuration.class) {
            returnValue = getDuration(columnIndex);
        } else if (type == URL.class) {
            returnValue = getURL(columnIndex);
        } else if (type == CqlVector.class) {
            returnValue = getVector(columnIndex);
        } else {
            throw new SQLException(String.format(UNSUPPORTED_TYPE_CONVERSION, type.getSimpleName()));
        }

        return type.cast(returnValue);
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
    public <T> T getObjectFromJson(final int columnIndex, final Class<T> type) throws SQLException {
        final String json = getString(columnIndex);
        if (json != null) {
            try {
                return getObjectMapper().readValue(json, type);
            } catch (final JsonProcessingException e) {
                throw new SQLException(String.format(UNSUPPORTED_JSON_TYPE_CONVERSION, columnIndex, type.getName()), e);
            }
        }
        return null;
    }

    @Override
    public <T> T getObjectFromJson(final String columnLabel, final Class<T> type) throws SQLException {
        final int index = findColumn(columnLabel);
        return getObjectFromJson(index, type);
    }

    @Override
    public <T> T getObjectFromJson(final Class<T> type) throws SQLException {
        return getObjectFromJson("[json]", type);
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
                Class.forName(fromDataType(setType.getElementType()).asJavaClass().getCanonicalName()));
        } catch (ClassNotFoundException e) {
            LOG.warn("Error while executing getSet()", e);
        }
        return null;
    }

    @Override
    public Set<?> getSet(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        try {
            final SetType setType = (SetType) getCqlDataType(columnLabel);
            return this.currentRow.getSet(columnLabel,
                Class.forName(fromDataType(setType.getElementType()).asJavaClass().getCanonicalName()));
        } catch (ClassNotFoundException e) {
            LOG.warn("Error while executing getSet()", e);
        }
        return null;
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
            if (fromCqlTypeName(getCqlDataType(columnIndex).asCql(false, false)).isCollection()) {
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
            if (fromCqlTypeName(getCqlDataType(columnLabel).asCql(false, false)).isCollection()) {
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
        final LocalTime localTime = this.currentRow.getLocalTime(columnIndex - 1);
        if (localTime == null) {
            return null;
        }
        return Time.valueOf(localTime);
    }

    @Override
    public Time getTime(final int columnIndex, final Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; it's a hint we do not need
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final LocalTime localTime = this.currentRow.getLocalTime(columnLabel);
        if (localTime == null) {
            return null;
        }
        return Time.valueOf(localTime);
    }

    @Override
    public Time getTime(final String columnLabel, final Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; it's a hint we do not need
        return getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        final Instant instant = this.currentRow.getInstant(columnIndex - 1);
        if (instant == null) {
            return null;
        }
        return Timestamp.from(instant);
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; it's a hint we do not need
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        final Instant instant = this.currentRow.getInstant(columnLabel);
        if (instant == null) {
            return null;
        }
        return Timestamp.from(instant);
    }

    public Timestamp getTimestamp(final String columnLabel, final Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; it's a hint we do not need
        return getTimestamp(columnLabel);
    }

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
                return new URL(storedUrl);
            } catch (final MalformedURLException e) {
                throw new SQLException(String.format(MALFORMED_URL, storedUrl), e);
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
                return new URL(storedUrl);
            } catch (final MalformedURLException e) {
                throw new SQLException(String.format(MALFORMED_URL, storedUrl), e);
            }
        }
    }

    @Override
    public CqlVector<?> getVector(final int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        try {
            final VectorType vectorType = (VectorType) getCqlDataType(columnIndex);
            final Class<?> elementClass = Class.forName(fromDataType(vectorType.getElementType()).asJavaClass()
                .getCanonicalName());
            if (Number.class.isAssignableFrom(elementClass)) {
                return this.currentRow.getVector(columnIndex - 1, elementClass.asSubclass(Number.class));
            } else {
                throw new SQLException(VECTOR_ELEMENTS_NOT_NUMBERS);
            }
        } catch (ClassNotFoundException e) {
            LOG.warn("Error while executing getSet()", e);
        }
        return null;
    }

    @Override
    public CqlVector<?> getVector(final String columnLabel) throws SQLException {
        checkName(columnLabel);
        try {
            final VectorType vectorType = (VectorType) getCqlDataType(columnLabel);
            final Class<?> elementClass = Class.forName(fromDataType(vectorType.getElementType()).asJavaClass()
                .getCanonicalName());
            if (Number.class.isAssignableFrom(elementClass)) {
                return this.currentRow.getVector(columnLabel, elementClass.asSubclass(Number.class));
            } else {
                throw new SQLException(VECTOR_ELEMENTS_NOT_NUMBERS);
            }
        } catch (ClassNotFoundException e) {
            LOG.warn("Error while executing getVector()", e);
        }
        return null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkNotClosed();
        final List<String> driverWarnings = this.driverResultSet.getExecutionInfo().getWarnings();
        if (!driverWarnings.isEmpty()) {
            SQLWarning firstWarning = null;
            SQLWarning previousWarning = null;

            for (final String warningMessage : driverWarnings) {
                final SQLWarning warning = new SQLWarning(warningMessage);
                if (previousWarning == null) {
                    firstWarning = warning;
                } else {
                    previousWarning.setNextWarning(warning);
                }
                previousWarning = warning;
            }

            return firstWarning;
        } else {
            return null;
        }
    }

    /**
     * Gets whether this result set has still rows to iterate over.
     *
     * @return {@code true} if there is still rows to iterate over, {@code false} otherwise.
     */
    protected boolean hasMoreRows() {
        return this.rowsIterator != null
            && (this.rowsIterator.hasNext() || (this.rowNumber == 0 && this.currentRow != null));
    }

    boolean isQuery() {
        return this.driverResultSet != null && this.driverResultSet.getColumnDefinitions().size() > 0;
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

    /**
     * Gets whether a column was a null value.
     *
     * @return {@code true} if the column contained a {@code null} value, {@code false} otherwise.
     */
    public boolean wasNull() {
        return this.wasNull;
    }

    private <T> T valueOrNull(final T value) {
        if (wasNull()) {
            return null;
        } else {
            return value;
        }
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
        public String getColumnClassName(final int column) {
            if (currentRow != null) {
                return fromCqlTypeName(getCqlDataType(column).asCql(false, false)).asJavaClass().getCanonicalName();
            }
            return fromCqlTypeName(
                driverResultSet.getColumnDefinitions().get(column - 1).getType().asCql(false, false))
                .asJavaClass().getCanonicalName();
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
        public int getColumnDisplaySize(final int column) {
            try {
                final AbstractJdbcType<?> jdbcEquivalentType;
                final ColumnDefinition columnDefinition;
                if (currentRow != null) {
                    columnDefinition = currentRow.getColumnDefinitions().get(column - 1);
                } else {
                    columnDefinition = driverResultSet.getColumnDefinitions().get(column - 1);
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
        public String getColumnLabel(final int column) {
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
            final DataType dataType;
            if (currentRow != null) {
                dataType = currentRow.getColumnDefinitions().get(column - 1).getType();
            } else {
                dataType = driverResultSet.getColumnDefinitions().get(column - 1).getType();
            }
            return TypesMap.getTypeForComparator(dataType.toString()).getJdbcType();
        }

        @Override
        public String getColumnTypeName(final int column) {
            // Specification says "database specific type name"; for Cassandra this means the AbstractType.
            final DataType dataType;
            if (currentRow != null) {
                dataType = currentRow.getColumnDefinitions().get(column - 1).getType();
            } else {
                dataType = driverResultSet.getColumnDefinitions().get(column - 1).getType();
            }
            if (dataType.toString().contains(DataTypeEnum.VECTOR.cqlType)) {
                return DataTypeEnum.VECTOR.cqlType;
            }
            return dataType.toString();
        }

        @Override
        public int getPrecision(final int column) {
            return Math.max(getColumnDisplaySize(column), 0);
        }

        @Override
        public int getScale(final int column) {
            try {
                final AbstractJdbcType<?> jdbcEquivalentType;
                final ColumnDefinition columnDefinition;
                if (currentRow != null) {
                    columnDefinition = currentRow.getColumnDefinitions().get(column - 1);
                } else {
                    columnDefinition = driverResultSet.getColumnDefinitions().get(column - 1);
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
            if (currentRow != null) {
                tableName = currentRow.getColumnDefinitions().get(column - 1).getTable().asInternal();
            } else {
                tableName = driverResultSet.getColumnDefinitions().get(column - 1).getTable().asInternal();
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
            // If the schema or table name is not defined (this should not happen here, but better to be careful),
            // always returns false since we cannot determine if the column is searchable in this context.
            if (StringUtils.isEmpty(schemaName) || StringUtils.isEmpty(tableName)) {
                return false;
            }
            final AtomicBoolean searchable = new AtomicBoolean(false);
            statement.connection.getSession().getMetadata().getKeyspace(schemaName)
                .flatMap(metadata -> metadata.getTable(tableName))
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
        public boolean isSigned(final int column) {
            final DataType dataType;
            if (currentRow != null) {
                dataType = currentRow.getColumnDefinitions().get(column - 1).getType();
            } else {
                dataType = driverResultSet.getColumnDefinitions().get(column - 1).getType();
            }
            return TypesMap.getTypeForComparator(dataType.toString()).isSigned();
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
                throw new SQLException(String.format(NO_INTERFACE, iface.getSimpleName()));
            }
        }
    }

}
