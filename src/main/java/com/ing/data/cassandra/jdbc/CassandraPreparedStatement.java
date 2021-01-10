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
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.internal.core.type.DefaultListType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.DefaultSetType;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Blob;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Cassandra prepared statement: implementation class for {@link PreparedStatement}.
 * <p>
 *     It extends {@link CassandraStatement} and thereby also implements {@link CassandraStatementExtras} interface
 *     providing extra methods not defined in JDBC API to manage some properties specific to the Cassandra statements
 *     (e.g. consistency level).
 * </p>
 */
public class CassandraPreparedStatement extends CassandraStatement implements PreparedStatement {

    private static final Logger log = LoggerFactory.getLogger(CassandraPreparedStatement.class);

    // The count of bound variable markers ('?') encountered during the parsing of the CQL server-side.
    private final int count;
    // A map of the current bound values encountered in setXXX() methods.
    private final Map<Integer, Object> bindValues = new LinkedHashMap<>();
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
     */
    CassandraPreparedStatement(final CassandraConnection connection, final String cql, final int resultSetType,
                               final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
        super(connection, cql, resultSetType, resultSetConcurrency, resultSetHoldability);
        if (log.isTraceEnabled() || connection.isDebugMode()) {
            log.trace("CQL: " + this.cql);
        }
        try {
            this.preparedStatement = getCqlSession().prepare(cql);
            this.boundStatement = this.preparedStatement.boundStatementBuilder().build();
            this.batchStatements = Lists.newArrayList();
            this.count = cql.length() - cql.replace("?", StringUtils.EMPTY).length();
        } catch (final Exception e) {
            throw new SQLTransientException(e);
        }
    }

    private void checkIndex(final int index) throws SQLException {
        if (index > this.count) {
            throw new SQLRecoverableException(String.format(
                "The column index: %d is greater than the count of bound variable markers in the CQL: %d", index,
                this.count));
        }
        if (index < 1) {
            throw new SQLRecoverableException(String.format("The column index must be a positive number: %d", index));
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
            log.warn("Unable to close the prepared statement: " + e.getMessage());
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void doExecute() throws SQLException {
        try {
            resetResults();
            if (log.isTraceEnabled() || this.connection.isDebugMode()) {
                log.trace("CQL: " + this.cql);
            }
            // Force paging to avoid timeout and node harm.
            if (this.boundStatement.getPageSize() == 0) {
                this.boundStatement.setPageSize(DEFAULT_FETCH_SIZE);
            }
            this.boundStatement.setConsistencyLevel(this.connection.getDefaultConsistencyLevel());
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
            throw new SQLNonTransientException("Too many queries at once (" + batchStatements.size() + "). You must "
                + "split your queries into more batches!");
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        checkNotClosed();
        this.bindValues.clear();
    }

    @Override
    public boolean execute() throws SQLException {
        checkNotClosed();
        doExecute();
        // Return true if the first result is a non-null ResultSet object; false if the first result is an update count
        // or there is no result.
        return this.currentResultSet != null && ((CassandraResultSet) this.currentResultSet).hasMoreRows();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public int[] executeBatch() throws SQLException {
        final int[] returnCounts = new int[this.batchStatements.size()];
        try {
            final List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>();
            if (log.isTraceEnabled() || this.connection.isDebugMode()) {
                log.trace("CQL statements: " + this.batchStatements.size());
            }
            for (final BoundStatement statement : this.batchStatements) {
                for (int i = 0; i < statement.getPreparedStatement().getVariableDefinitions().size(); i++) {
                    // Set parameters to null if unset.
                    if (!statement.isSet(i)) {
                        statement.setToNull(i);
                    }
                }
                if (log.isTraceEnabled() || this.connection.isDebugMode()) {
                    log.trace("CQL: " + this.cql);
                }
                final BoundStatement boundStatement = statement.setConsistencyLevel(
                    this.connection.getDefaultConsistencyLevel());
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
            this.batchStatements = Lists.newArrayList();
        } catch (final Exception e) {
            // Empty the batch statement list after execution even if it failed.
            this.batchStatements = Lists.newArrayList();
            throw new SQLTransientException(e);
        }

        return returnCounts;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkNotClosed();
        doExecute();
        if (this.currentResultSet == null) {
            throw new SQLNonTransientException(Utils.NO_RESULT_SET);
        }
        return this.currentResultSet;
    }

    /**
     * Executes the CQL statement in this {@link PreparedStatement} object, which must be a CQL Data Manipulation
     * Language (DML) statement, such as {@code INSERT}, {@code UPDATE} or {@code DELETE}; or a CQL statement that
     * returns nothing, such as a DDL statement.
     *
     * @return Always 0, for any statement. The rationale is that Datastax Java driver does not provide update count.
     * @throws SQLException when something went wrong during the execution of the statement.
     */
    @Override
    public int executeUpdate() throws SQLException {
        checkNotClosed();
        doExecute();
        // There is no updateCount available in Datastax Java driver, so return 0.
        return 0;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        // TODO: method to implement
        return null;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        // TODO: method to implement
        return null;
    }

    @Override
    public void setBigDecimal(final int parameterIndex, final BigDecimal x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.boundStatement = this.boundStatement.setBigDecimal(parameterIndex - 1, x);
    }

    @Override
    public void setBlob(final int parameterIndex, final Blob x) throws SQLException {
        final InputStream in = x.getBinaryStream();
        setBlob(parameterIndex, in);
    }

    @Override
    public void setBlob(final int parameterIndex, final InputStream inputStream) throws SQLException {
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

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader, final int length)
        throws SQLException {
        try {
            final String targetString = CharStreams.toString(reader);
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
        this.boundStatement = this.boundStatement.setLocalDate(parameterIndex - 1, x.toLocalDate());
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
        int targetType;
        if (x.getClass().equals(java.lang.Long.class)) {
            targetType = Types.BIGINT;
        } else if (x.getClass().equals(java.io.ByteArrayInputStream.class)) {
            targetType = Types.BINARY;
        } else if (x.getClass().equals(java.lang.String.class)) {
            targetType = Types.VARCHAR;
        } else if (x.getClass().equals(java.lang.Boolean.class)) {
            targetType = Types.BOOLEAN;
        } else if (x.getClass().equals(java.math.BigDecimal.class)) {
            targetType = Types.DECIMAL;
        } else if (x.getClass().equals(java.math.BigInteger.class)) {
            targetType = Types.BIGINT;
        } else if (x.getClass().equals(java.lang.Double.class)) {
            targetType = Types.DOUBLE;
        } else if (x.getClass().equals(java.lang.Float.class)) {
            targetType = Types.FLOAT;
        } else if (x.getClass().equals(java.net.Inet4Address.class)) {
            targetType = Types.OTHER;
        } else if (x.getClass().equals(java.lang.Integer.class)) {
            targetType = Types.INTEGER;
        } else if (x.getClass().equals(java.sql.Timestamp.class)) {
            targetType = Types.TIMESTAMP;
        } else if (x.getClass().equals(java.util.UUID.class)) {
            targetType = Types.ROWID;
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
                final byte[] array = new byte[((java.io.ByteArrayInputStream) x).available()];
                try {
                    ((java.io.ByteArrayInputStream) x).read(array);
                } catch (final IOException e) {
                    log.warn("Exception while setting object of BINARY type.", e);
                }
                this.boundStatement = this.boundStatement.setByteBuffer(parameterIndex - 1, ByteBuffer.wrap(array));
                break;
            case Types.BOOLEAN:
                this.boundStatement = this.boundStatement.setBoolean(parameterIndex - 1, (Boolean) x);
                break;
            case Types.CHAR:
            case Types.CLOB:
            case Types.VARCHAR:
                this.boundStatement = this.boundStatement.setString(parameterIndex - 1, x.toString());
                break;
            case Types.TIMESTAMP:
                this.boundStatement = this.boundStatement.setInstant(parameterIndex - 1, ((Timestamp) x).toInstant());
                break;
            case Types.DECIMAL:
                this.boundStatement = this.boundStatement.setBigDecimal(parameterIndex - 1, (BigDecimal) x);
                break;
            case Types.DOUBLE:
                this.boundStatement = this.boundStatement.setDouble(parameterIndex - 1, (Double) x);
                break;
            case Types.FLOAT:
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
            case Types.DATE:
                this.boundStatement = this.boundStatement.setLocalDate(parameterIndex - 1, ((Date) x).toLocalDate());
                break;
            case Types.ROWID:
                this.boundStatement = this.boundStatement.setUuid(parameterIndex - 1, (java.util.UUID) x);
                break;
            case Types.OTHER:
                // TODO: replace x.getClass().equals(T.class) by x instanceof T ?
                if (x.getClass().equals(TupleValue.class)) {
                    this.boundStatement = this.boundStatement.setTupleValue(parameterIndex - 1, (TupleValue) x);
                }
                if (x.getClass().equals(java.util.UUID.class)) {
                    this.boundStatement = this.boundStatement.setUuid(parameterIndex - 1, (java.util.UUID) x);
                }
                if (x.getClass().equals(java.net.InetAddress.class)
                    || x.getClass().equals(java.net.Inet4Address.class)) {
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
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void setRowId(final int parameterIndex, final RowId x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.boundStatement.setToNull(parameterIndex - 1);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void setShort(final int parameterIndex, final short x) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.boundStatement.setInt(parameterIndex - 1, x);
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
        // Time data type is handled as an 8 byte Long value of milliseconds since the epoch.
        Object timeValue = null;
        if (x != null) {
           timeValue = JdbcLong.instance.decompose(x.getTime());
        }
        this.bindValues.put(parameterIndex, timeValue);
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
        // Timestamp data type is handled as an 8 byte Long value of milliseconds since the epoch.Nanos are not
        // supported and are ignored.
        this.boundStatement = this.boundStatement.setInstant(parameterIndex - 1, x.toInstant());
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
    public ResultSet getResultSet() throws SQLException {
        checkNotClosed();
        return this.currentResultSet;
    }

}
