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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.internal.core.type.DefaultListType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.DefaultSetType;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.ing.data.cassandra.jdbc.types.DataTypeEnum;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static com.ing.data.cassandra.jdbc.utils.ConversionsUtil.convertToByteArray;
import static com.ing.data.cassandra.jdbc.utils.ConversionsUtil.convertToInstant;
import static com.ing.data.cassandra.jdbc.utils.ConversionsUtil.convertToLocalDate;
import static com.ing.data.cassandra.jdbc.utils.ConversionsUtil.convertToLocalTime;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.MUST_BE_POSITIVE_BINDING_INDEX;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NO_RESULT_SET;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.OUT_OF_BOUNDS_BINDING_INDEX;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.TOO_MANY_QUERIES;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.UNSUPPORTED_CONVERSION_TO_JSON;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.UNSUPPORTED_JDBC_TYPE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.VECTOR_ELEMENTS_NOT_NUMBERS;
import static com.ing.data.cassandra.jdbc.utils.JsonUtil.getObjectMapper;

/**
 * Cassandra prepared statement: implementation class for {@link PreparedStatement}.
 * <p>
 *     It extends {@link CassandraStatement} and thereby also implements {@link CassandraStatementExtras} and
 *     {@link CassandraStatementJsonSupport} interfaces providing extra methods not defined in JDBC API to manage
 *     some properties specific to the Cassandra statements (e.g. consistency level) and ease usage of JSON features
 *     {@code INSERT INTO ... JSON} and {@code fromJson()} provided by Cassandra.
 * </p>
 */
public class CassandraPreparedStatement extends CassandraStatement
    implements PreparedStatement, CassandraStatementJsonSupport {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraPreparedStatement.class);

    // The count of bound variable markers ('?') encountered during the parsing of the CQL server-side.
    private final int count;
    private final com.datastax.oss.driver.api.core.cql.PreparedStatement preparedStatement;
    private BoundStatement boundStatement;
    private ArrayList<BoundStatement> batchStatements;

    /**
     * Constructor. It instantiates a new Cassandra prepared statement with default values for a
     * {@link CassandraConnection}.
     * <p>
     *     By default, the result set type is {@link ResultSet#TYPE_FORWARD_ONLY}, the result set concurrency is
     *     {@link ResultSet#CONCUR_READ_ONLY} and the result set holdability is
     *     {@link ResultSet#HOLD_CURSORS_OVER_COMMIT}.
     * </p>
     *
     * @param connection    The Cassandra connection to the database.
     * @param cql           The CQL statement.
     * @throws SQLException when something went wrong during the initialisation of the statement.
     */
    CassandraPreparedStatement(final CassandraConnection connection, final String cql) throws SQLException {
        this(connection, cql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Constructor. It instantiates a new Cassandra prepared statement for a {@link CassandraConnection}.
     *
     * @param connection           The {@link CassandraConnection} instance.
     * @param cql                  The CQL statement.
     * @param resultSetType        The result set type.
     * @param resultSetConcurrency The result set concurrency
     * @param resultSetHoldability The result set holdability.
     * @throws SQLException when something went wrong during the initialisation of the statement.
     * @throws SQLTransientException when something went wrong during the initialisation of the statement.
     */
    CassandraPreparedStatement(final CassandraConnection connection, final String cql, final int resultSetType,
                               final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
        super(connection, cql, resultSetType, resultSetConcurrency, resultSetHoldability);
        if (LOG.isTraceEnabled() || connection.isDebugMode()) {
            LOG.trace("CQL: {}", this.cql);
        }
        try {
            this.preparedStatement = getCqlSession().prepare(cql);
            this.boundStatement = this.preparedStatement.boundStatementBuilder().build();
            this.batchStatements = new ArrayList<>();
            this.count = cql.length() - cql.replace("?", StringUtils.EMPTY).length();
        } catch (final Exception e) {
            throw new SQLTransientException(e);
        }
    }

    private void checkIndex(final int index) throws SQLException {
        if (index > this.count) {
            throw new SQLRecoverableException(String.format(OUT_OF_BOUNDS_BINDING_INDEX, index, this.count));
        }
        if (index < 1) {
            throw new SQLRecoverableException(String.format(MUST_BE_POSITIVE_BINDING_INDEX, index));
        }
    }

    String getCql() {
        return this.cql;
    }

    CqlSession getCqlSession() {
        return (CqlSession) this.connection.getSession();
    }

    ColumnDefinitions getBoundStatementVariableDefinitions() {
        return this.boundStatement.getPreparedStatement().getVariableDefinitions();
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> handleAsList(final Class<?> objectClass, final Object object) {
        if (!List.class.isAssignableFrom(objectClass)) {
            return null;
        }
        return (List<T>) object.getClass().cast(object);
    }

    @SuppressWarnings("unchecked")
    private static <T> Set<T> handleAsSet(final Class<?> objectClass, final Object object) {
        if (!Set.class.isAssignableFrom(objectClass)) {
            return null;
        }
        return (Set<T>) object.getClass().cast(object);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> HashMap<K, V> handleAsMap(final Class<?> objectClass, final Object object) {
        if (!Map.class.isAssignableFrom(objectClass)) {
            return null;
        }
        return (HashMap<K, V>) object.getClass().cast(object);
    }

    @Override
    public void close() {
        try {
            this.connection.removeStatement(this);
        } catch (final Exception e) {
            LOG.warn("Unable to close the prepared statement: {}", e.getMessage());
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void doExecute() throws SQLException {
        try {
            resetResults();
            if (LOG.isTraceEnabled() || this.connection.isDebugMode()) {
                LOG.trace("CQL: {}", this.cql);
            }
            this.boundStatement = this.boundStatement
                .setPageSize(this.getFetchSize()) // Set paging to avoid timeout and node harm.
                .setConsistencyLevel(this.connection.getConsistencyLevel());
            for (int i = 0; i < getBoundStatementVariableDefinitions().size(); i++) {
                // Set parameters to null if unset.
                if (!this.boundStatement.isSet(i)) {
                    this.boundStatement.setToNull(i);
                }
            }
            this.currentResultSet = new CassandraResultSet(this, getCqlSession().execute(this.boundStatement));
        } catch (final Exception e) {
            throw new SQLTransientException(e);
        }
    }

    @Override
    public void addBatch() throws SQLException {
        this.batchStatements.add(this.boundStatement);
        this.boundStatement = this.preparedStatement.boundStatementBuilder().build();
        if (this.batchStatements.size() > MAX_ASYNC_QUERIES) {
            throw new SQLNonTransientException(String.format(TOO_MANY_QUERIES, batchStatements.size()));
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        checkNotClosed();
    }

    @Override
    public boolean execute() throws SQLException {
        checkNotClosed();
        doExecute();
        // Return true if the first result is a non-null ResultSet object; false if the first result is an update count
        // or there is no result.
        return this.currentResultSet != null && ((CassandraResultSet) this.currentResultSet).isQuery();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public int[] executeBatch() throws SQLException {
        final int[] returnCounts = new int[this.batchStatements.size()];
        try {
            final List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>();
            if (LOG.isTraceEnabled() || this.connection.isDebugMode()) {
                LOG.trace("CQL statements: {}", this.batchStatements.size());
            }
            for (final BoundStatement statement : this.batchStatements) {
                for (int i = 0; i < statement.getPreparedStatement().getVariableDefinitions().size(); i++) {
                    // Set parameters to null if unset.
                    if (!statement.isSet(i)) {
                        statement.setToNull(i);
                    }
                }
                if (LOG.isTraceEnabled() || this.connection.isDebugMode()) {
                    LOG.trace("CQL: {}", this.cql);
                }
                final BoundStatement boundStatement = statement.setConsistencyLevel(
                    this.connection.getConsistencyLevel());
                final CompletionStage<AsyncResultSet> resultSetFuture = getCqlSession().executeAsync(boundStatement);
                futures.add(resultSetFuture);
            }

            int i = 0;
            for (final CompletionStage<AsyncResultSet> future : futures) {
                CompletableFutures.getUninterruptibly(future);
                returnCounts[i] = 1;
                i++;
            }

            // Empty the batch statement list after execution.
            this.batchStatements = new ArrayList<>();
        } catch (final Exception e) {
            // Empty the batch statement list after execution even if it failed.
            this.batchStatements = new ArrayList<>();
            throw new SQLTransientException(e);
        }

        return returnCounts;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkNotClosed();
        doExecute();
        if (this.currentResultSet == null) {
            throw new SQLNonTransientException(NO_RESULT_SET);
        }
        return this.currentResultSet;
    }

    /**
     * Executes the CQL statement in this {@link PreparedStatement} object, which must be a CQL Data Manipulation
     * Language (DML) statement, such as {@code INSERT}, {@code UPDATE} or {@code DELETE}; or a CQL statement that
     * returns nothing, such as a DDL statement.
     *
     * @return Always 0, for any statement. The rationale is that Java Driver for Apache Cassandra® does not provide
     * update count.
     * @throws SQLException when something went wrong during the execution of the statement.
     */
    @Override
    public int executeUpdate() throws SQLException {
        checkNotClosed();
        doExecute();
        // There is no updateCount available in Java Driver for Apache Cassandra®, so return 0.
        return 0;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return this.getResultSet().getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return new CassandraParameterMetaData(this.boundStatement, this.count);
    }

    @Override
    public void setBigDecimal(final int parameterIndex, final BigDecimal x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        if (x == null) {
            this.boundStatement = this.boundStatement.setToNull(parameterIndex - 1);
        } else {
            this.boundStatement = this.boundStatement.setBigDecimal(parameterIndex - 1, x);
        }
    }

    @Override
    public void setBlob(final int parameterIndex, final Blob x) throws SQLException {
        final InputStream in = x.getBinaryStream();
        setBlob(parameterIndex, in);
    }

    @Override
    public void setBlob(final int parameterIndex, final InputStream inputStream) throws SQLException {
        if (inputStream == null) {
            this.boundStatement = this.boundStatement.setToNull(parameterIndex - 1);
        } else {
            int nRead;
            final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            final byte[] data = new byte[16384];

            try {
                while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
                    buffer.write(data, 0, nRead);
                }
            } catch (final IOException e) {
                throw new SQLNonTransientException(e);
            }

            try {
                buffer.flush();
            } catch (final IOException e) {
                throw new SQLNonTransientException(e);
            }

            this.boundStatement = this.boundStatement.setByteBuffer(parameterIndex - 1,
                ByteBuffer.wrap(buffer.toByteArray()));
        }
    }

    @Override
    public void setBoolean(final int parameterIndex, final boolean x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.boundStatement = this.boundStatement.setBoolean(parameterIndex - 1, x);
    }

    @Override
    public void setByte(final int parameterIndex, final byte x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.boundStatement = this.boundStatement.setByte(parameterIndex - 1, x);
    }

    @Override
    public void setBytes(final int parameterIndex, final byte[] x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.boundStatement = this.boundStatement.setByteBuffer(parameterIndex - 1, ByteBuffer.wrap(x));
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader, final int length)
        throws SQLException {
        try {
            final String targetString = IOUtils.toString(reader);
            reader.close();
            setString(parameterIndex, targetString);
        } catch (final IOException e) {
            throw new SQLNonTransientException(e);
        }
    }

    @Override
    public void setDate(final int parameterIndex, final Date x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        if (x == null) {
            this.boundStatement = this.boundStatement.setToNull(parameterIndex - 1);
        } else {
            this.boundStatement = this.boundStatement.setLocalDate(parameterIndex - 1, x.toLocalDate());
        }
    }

    @Override
    public void setDate(final int parameterIndex, final Date x, final Calendar cal) throws SQLException {
        // Silently ignore the Calendar argument; it is not useful for the Cassandra implementation.
        setDate(parameterIndex, x);
    }

    @Override
    public void setDouble(final int parameterIndex, final double x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.boundStatement = this.boundStatement.setDouble(parameterIndex - 1, x);
    }

    /**
     * Sets the designated parameter to the given {@link CqlDuration} value.
     * <p>
     *     This method is not JDBC standard but it helps to set {@code DURATION} CQL values into a prepared statement.
     * </p>
     *
     * @param parameterIndex    The first parameter is 1, the second is 2, ...
     * @param x                 The parameter value.
     * @throws SQLException if {@code parameterIndex} does not correspond to a parameter marker in the CQL statement;
     * if a database access error occurs or this method is called on a closed {@link PreparedStatement}.
     */
    public void setDuration(final int parameterIndex, final CqlDuration x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        if (x == null) {
            this.boundStatement = this.boundStatement.setToNull(parameterIndex - 1);
        } else {
            this.boundStatement = this.boundStatement.setCqlDuration(parameterIndex - 1, x);
        }
    }

    @Override
    public void setFloat(final int parameterIndex, final float x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.boundStatement = this.boundStatement.setFloat(parameterIndex - 1, x);
    }

    @Override
    public void setInt(final int parameterIndex, final int x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        if (DataTypeEnum.VARINT.cqlType.equals(
            getBoundStatementVariableDefinitions().get(parameterIndex - 1).getType().asCql(false, false))) {
            this.boundStatement = this.boundStatement.setBigInteger(parameterIndex - 1,
                BigInteger.valueOf(Long.parseLong(String.valueOf(x))));
        } else {
            this.boundStatement = this.boundStatement.setInt(parameterIndex - 1, x);
        }
    }

    @Override
    public void setLong(final int parameterIndex, final long x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.boundStatement = this.boundStatement.setLong(parameterIndex - 1, x);
    }

    @Override
    public void setNString(final int parameterIndex, final String value) throws SQLException {
        // Treat this like a String.
        setString(parameterIndex, value);
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.boundStatement = this.boundStatement.setToNull(parameterIndex - 1);
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType, final String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setObject(final int parameterIndex, final Object x) throws SQLException {
        final int targetType;
        if (x.getClass().equals(Long.class)) {
            targetType = Types.BIGINT;
        } else if (x.getClass().equals(ByteArrayInputStream.class)) {
            targetType = Types.BINARY;
        } else if (x instanceof byte[]) {
            targetType = Types.VARBINARY;
        } else if (x instanceof Blob) {
            targetType = Types.BLOB;
        } else if (x instanceof Clob) {
            targetType = Types.CLOB;
        } else if (x.getClass().equals(String.class)) {
            targetType = Types.VARCHAR;
        } else if (x.getClass().equals(Boolean.class)) {
            targetType = Types.BOOLEAN;
        } else if (x.getClass().equals(BigDecimal.class)) {
            targetType = Types.DECIMAL;
        } else if (x.getClass().equals(BigInteger.class)) {
            targetType = Types.BIGINT;
        } else if (x.getClass().equals(Double.class)) {
            targetType = Types.DOUBLE;
        } else if (x.getClass().equals(Float.class)) {
            targetType = Types.FLOAT;
        } else if (x.getClass().equals(Inet4Address.class)) {
            targetType = Types.OTHER;
        } else if (x.getClass().equals(Integer.class)) {
            targetType = Types.INTEGER;
        } else if (x.getClass().equals(java.sql.Timestamp.class) || x instanceof Calendar
            || x.getClass().equals(java.util.Date.class) || x.getClass().equals(LocalDateTime.class)) {
            targetType = Types.TIMESTAMP;
        } else if (x.getClass().equals(java.sql.Date.class) || x.getClass().equals(LocalDate.class)) {
            targetType = Types.DATE;
        } else if (x.getClass().equals(java.sql.Time.class) || x.getClass().equals(LocalTime.class)) {
            targetType = Types.TIME;
        } else if (x.getClass().equals(OffsetDateTime.class)) {
            targetType = Types.TIMESTAMP_WITH_TIMEZONE;
        } else if (x.getClass().equals(OffsetTime.class)) {
            targetType = Types.TIME_WITH_TIMEZONE;
        } else if (x.getClass().equals(Byte.class)) {
            targetType = Types.TINYINT;
        } else if (x.getClass().equals(Short.class)) {
            targetType = Types.SMALLINT;
        } else if (x.getClass().equals(URL.class)) {
            targetType = Types.DATALINK;
        } else if (x.getClass().equals(CqlDuration.class)) {
            targetType = Types.OTHER;
        } else if (x.getClass().equals(UUID.class)) {
            targetType = Types.OTHER;
        } else if (x.getClass().equals(CqlVector.class)) {
            targetType = Types.OTHER;
        } else {
            targetType = Types.OTHER;
        }
        setObject(parameterIndex, x, targetType, 0);
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType) throws SQLException {
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    @SuppressWarnings({"ResultOfMethodCallIgnored", "unchecked", "rawtypes"})
    @Override
    public final void setObject(final int parameterIndex, final Object x, final int targetSqlType,
                                final int scaleOrLength) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);

        switch (targetSqlType) {
            case Types.BIGINT:
                if (x instanceof BigInteger) {
                    this.boundStatement = this.boundStatement.setBigInteger(parameterIndex - 1, (BigInteger) x);
                } else {
                    this.boundStatement = this.boundStatement.setLong(parameterIndex - 1, Long.parseLong(x.toString()));
                }
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.NCLOB:
                final byte[] array = convertToByteArray(x);
                this.boundStatement = this.boundStatement.setByteBuffer(parameterIndex - 1, ByteBuffer.wrap(array));
                break;
            case Types.BOOLEAN:
            case Types.BIT:
                this.boundStatement = this.boundStatement.setBoolean(parameterIndex - 1, (Boolean) x);
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.DATALINK:
                this.boundStatement = this.boundStatement.setString(parameterIndex - 1, x.toString());
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                this.boundStatement = this.boundStatement.setInstant(parameterIndex - 1, convertToInstant(x));
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                this.boundStatement = this.boundStatement.setBigDecimal(parameterIndex - 1, (BigDecimal) x);
                break;
            case Types.DOUBLE:
                this.boundStatement = this.boundStatement.setDouble(parameterIndex - 1, (Double) x);
                break;
            case Types.FLOAT:
            case Types.REAL:
                this.boundStatement = this.boundStatement.setFloat(parameterIndex - 1, (Float) x);
                break;
            case Types.INTEGER:
                // If the value is an integer but the target CQL type is VARINT, cast to BigInteger.
                if (DataTypeEnum.VARINT.cqlType.equals(
                    getBoundStatementVariableDefinitions().get(parameterIndex - 1).getType().asCql(false, false))) {
                    this.boundStatement = this.boundStatement.setBigInteger(parameterIndex - 1,
                        BigInteger.valueOf(Long.parseLong(x.toString())));
                } else {
                    this.boundStatement = this.boundStatement.setInt(parameterIndex - 1, (Integer) x);
                }
                break;
            case Types.SMALLINT:
                this.boundStatement = this.boundStatement.setShort(parameterIndex - 1, (Short) x);
                break;
            case Types.TINYINT:
                this.boundStatement = this.boundStatement.setByte(parameterIndex - 1, (Byte) x);
                break;
            case Types.DATE:
                this.boundStatement = this.boundStatement.setLocalDate(parameterIndex - 1, convertToLocalDate(x));
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                this.boundStatement = this.boundStatement.setLocalTime(parameterIndex - 1, convertToLocalTime(x));
                break;
            case Types.ROWID:
                this.boundStatement.setToNull(parameterIndex - 1);
                break;
            case Types.OTHER:
                if (x instanceof TupleValue) {
                    this.boundStatement = this.boundStatement.setTupleValue(parameterIndex - 1, (TupleValue) x);
                } else if (x instanceof UUID) {
                    this.boundStatement = this.boundStatement.setUuid(parameterIndex - 1, (UUID) x);
                } else if (x instanceof CqlDuration) {
                    this.boundStatement = this.boundStatement.setCqlDuration(parameterIndex - 1, (CqlDuration) x);
                } else if (x instanceof CqlVector) {
                    final CqlVector vector = (CqlVector) x;
                    final DefaultVectorType vectorType =
                        (DefaultVectorType) getBoundStatementVariableDefinitions().get(parameterIndex - 1).getType();
                    final Class<?> itemsClass = DataTypeEnum.fromCqlTypeName(vectorType.getElementType()
                        .asCql(false, false)).javaType;
                    if (Number.class.isAssignableFrom(itemsClass)) {
                        this.boundStatement = this.boundStatement.setVector(parameterIndex - 1, vector,
                            itemsClass.asSubclass(Number.class));
                    } else {
                        throw new SQLException(VECTOR_ELEMENTS_NOT_NUMBERS);
                    }
                } else if (x instanceof java.net.InetAddress) {
                    this.boundStatement = this.boundStatement.setInetAddress(parameterIndex - 1, (InetAddress) x);
                } else if (List.class.isAssignableFrom(x.getClass())) {
                    final List handledList = handleAsList(x.getClass(), x);
                    final DefaultListType listType =
                        (DefaultListType) getBoundStatementVariableDefinitions().get(parameterIndex - 1).getType();
                    final Class<?> itemsClass = DataTypeEnum.fromCqlTypeName(listType.getElementType()
                        .asCql(false, false)).javaType;
                    this.boundStatement = this.boundStatement.setList(parameterIndex - 1, handledList, itemsClass);
                } else if (Set.class.isAssignableFrom(x.getClass())) {
                    final Set handledSet = handleAsSet(x.getClass(), x);
                    final DefaultSetType setType =
                        (DefaultSetType) getBoundStatementVariableDefinitions().get(parameterIndex - 1).getType();
                    final Class<?> itemsClass = DataTypeEnum.fromCqlTypeName(setType.getElementType()
                        .asCql(false, false)).javaType;
                    this.boundStatement = this.boundStatement.setSet(parameterIndex - 1, handledSet, itemsClass);
                } else if (Map.class.isAssignableFrom(x.getClass())) {
                    final Map handledMap = handleAsMap(x.getClass(), x);
                    final DefaultMapType mapType =
                        (DefaultMapType) getBoundStatementVariableDefinitions().get(parameterIndex - 1).getType();
                    final Class<?> keysClass = DataTypeEnum.fromCqlTypeName(mapType.getKeyType()
                        .asCql(false, false)).javaType;
                    final Class<?> valuesClass = DataTypeEnum.fromCqlTypeName(mapType.getValueType()
                        .asCql(false, false)).javaType;
                    this.boundStatement = this.boundStatement.setMap(parameterIndex - 1, handledMap, keysClass,
                        valuesClass);
                }
                break;
            default:
                throw new SQLException(String.format(UNSUPPORTED_JDBC_TYPE, targetSqlType));
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void setRowId(final int parameterIndex, final RowId x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.boundStatement.setToNull(parameterIndex - 1);
    }

    @Override
    public void setShort(final int parameterIndex, final short x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.boundStatement = this.boundStatement.setShort(parameterIndex - 1, x);
    }

    @Override
    public void setString(final int parameterIndex, final String x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.boundStatement = this.boundStatement.setString(parameterIndex - 1, x);
    }

    @Override
    public void setTime(final int parameterIndex, final Time x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        if (x == null) {
            this.boundStatement = this.boundStatement.setToNull(parameterIndex - 1);
        } else {
            this.boundStatement = this.boundStatement.setLocalTime(parameterIndex - 1, x.toLocalTime());
        }
    }

    @Override
    public void setTime(final int parameterIndex, final Time x, final Calendar cal) throws SQLException {
        // Silently ignore the Calendar argument; it is not useful for the Cassandra implementation.
        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        if (x == null) {
            this.boundStatement = this.boundStatement.setToNull(parameterIndex - 1);
        } else {
            this.boundStatement = this.boundStatement.setInstant(parameterIndex - 1, x.toInstant());
        }
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal) throws SQLException {
        // Silently ignore the Calendar argument; it is not useful for the Cassandra implementation.
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setURL(final int parameterIndex, final URL value) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        // URL data type is handled as a String.
        final String url = value.toString();
        setString(parameterIndex, url);
    }

    @Override
    public <T> void setJson(final int parameterIndex, final T x) throws SQLException {
        checkNotClosed();
        try {
            final String json = getObjectMapper().writeValueAsString(x);
            setString(parameterIndex, json);
        } catch (final JsonProcessingException e) {
            throw new SQLException(
                String.format(UNSUPPORTED_CONVERSION_TO_JSON, x.getClass().getName(), parameterIndex));
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkNotClosed();
        return this.currentResultSet;
    }

}
