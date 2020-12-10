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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

class CassandraPreparedStatement extends CassandraStatement implements PreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(CassandraPreparedStatement.class);

    /**
     * The count of bound variable markers (?) encountered in the parse of the CQL server-side.
     */
    private final int count;

    /**
     * A map of the current bound values encountered in setXXX() methods.
     */
    private final Map<Integer, Object> bindValues = new LinkedHashMap<Integer, Object>();
    private final com.datastax.oss.driver.api.core.cql.PreparedStatement stmt;

    private BoundStatement statement;
    private ArrayList<BoundStatement> batchStatements;
    protected ResultSet currentResultSet = null;

    /**
     * Constructor.
     *
     * @param con The {@link CassandraConnection} instance.
     * @param cql The CQL statement.
     * @throws SQLException when something went wrong during the initialisation of the statement.
     */
    CassandraPreparedStatement(final CassandraConnection con, final String cql) throws SQLException {
        this(con, cql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Constructor.
     *
     * @param con           The {@link CassandraConnection} instance.
     * @param cql           The CQL statement.
     * @param rsType        The result set type.
     * @param rsConcurrency The result set concurrency
     * @param rsHoldability The result set holdability.
     * @throws SQLException when something went wrong during the initialisation of the statement.
     */
    CassandraPreparedStatement(final CassandraConnection con, final String cql, final int rsType,
                               final int rsConcurrency, final int rsHoldability) throws SQLException {
        super(con, cql, rsType, rsConcurrency, rsHoldability);

        if (log.isTraceEnabled()) {
            log.trace("CQL: " + this.cql);
        }
        try {
            stmt = ((CqlSession) this.connection.getSession()).prepare(cql);
            this.statement = stmt.boundStatementBuilder().build();
            batchStatements = Lists.newArrayList();
            count = cql.length() - cql.replace("?", StringUtils.EMPTY).length();
        } catch (final Exception e) {
            throw new SQLTransientException(e);
        }
    }

    String getCql() {
        return cql;
    }

    @SuppressWarnings("boxing")
    private void checkIndex(final int index) throws SQLException {
        if (index > count) {
            throw new SQLRecoverableException(String.format(
                "the column index : %d is greater than the count of bound variable markers in the CQL: %d", index,
                count));
        }
        if (index < 1) {
            throw new SQLRecoverableException(String.format("the column index must be a positive number : %d", index));
        }
    }

    @Override
    public void close() {
        try {
            connection.removeStatement(this);
        } catch (final Exception e) {
            log.warn("Unable to close the prepared statement: " + e.getMessage());
        }
    }

    private void doExecute() throws SQLException {
        if (log.isTraceEnabled()) {
            log.trace("CQL: " + cql);
        }
        try {
            resetResults();
            if (this.connection.debugMode) {
                System.out.println("CQL: " + cql);
            }
            if (this.statement.getPageSize() == 0) {
                // force paging to avoid timeout and node harm...
                this.statement.setPageSize(100);
            }
            this.statement.setConsistencyLevel(this.connection.defaultConsistencyLevel);
            for (int i = 0; i < this.statement.getPreparedStatement().getVariableDefinitions().size(); i++) {
                // Set parameters to null if unset
                if (!this.statement.isSet(i)) {
                    this.statement.setToNull(i);
                }
            }
            currentResultSet = new CassandraResultSet(this, ((CqlSession) this.connection.getSession())
                .execute(this.statement));
        } catch (final Exception e) {
            throw new SQLTransientException(e);
        }
    }

    @Override
    public void addBatch() throws SQLException {
        batchStatements.add(statement);
        this.statement = stmt.boundStatementBuilder().build();
        if (batchStatements.size() > MAX_ASYNC_QUERIES) {
            throw new SQLNonTransientException("Too many queries at once (" + batchStatements.size() + "). You must "
                + "split your queries into more batches !");
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        final int[] returnCounts = new int[batchStatements.size()];
        try {
            final List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>();
            if (this.connection.debugMode) System.out.println("CQL statements : " + batchStatements.size());
            for (final BoundStatement q : batchStatements) {
                for (int i = 0; i < q.getPreparedStatement().getVariableDefinitions().size(); i++) {
                    // Set parameters to null if unset
                    if (!q.isSet(i)) {
                        q.setToNull(i);
                    }
                }
                if (this.connection.debugMode) {
                    System.out.println("CQL: " + cql);
                }
                final BoundStatement boundStatement = q.setConsistencyLevel(this.connection.defaultConsistencyLevel);
                final CompletionStage<AsyncResultSet> resultSetFuture = ((CqlSession) this.connection.getSession())
                    .executeAsync(boundStatement);
                futures.add(resultSetFuture);
            }

            int i = 0;
            for (final CompletionStage<AsyncResultSet> future : futures) {
                CompletableFutures.getUninterruptibly(future);
                returnCounts[i] = 1;
                i++;
            }

            // empty batch statement list after execution
            batchStatements = Lists.newArrayList();
        } catch (final Exception e) {
            // empty batch statement list after execution even if it failed...
            batchStatements = Lists.newArrayList();
            throw new SQLTransientException(e);
        }

        return returnCounts;
    }

    @Override
    public void clearParameters() throws SQLException {
        checkNotClosed();
        bindValues.clear();
    }

    @Override
    public boolean execute() throws SQLException {
        checkNotClosed();
        doExecute();
        return !(currentResultSet == null);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkNotClosed();
        doExecute();
        if (currentResultSet == null) {
            throw new SQLNonTransientException(Utils.NO_RESULTSET);
        }
        return currentResultSet;
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkNotClosed();
        doExecute();
        // if (currentResultSet != null) throw new SQLNonTransientException(NO_UPDATE_COUNT);
        // no update count available with the Datastax java driver
        return 0;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setBigDecimal(final int parameterIndex, final BigDecimal decimal) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.statement = this.statement.setBigDecimal(parameterIndex - 1, decimal);
    }

    @Override
    public void setBoolean(final int parameterIndex, final boolean truth) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.statement = this.statement.setBoolean(parameterIndex - 1, truth);
    }

    @Override
    public void setByte(final int parameterIndex, final byte b) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.statement = this.statement.setByte(parameterIndex - 1, b);
    }

    @Override
    public void setBytes(final int parameterIndex, final byte[] bytes) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.statement = this.statement.setByteBuffer(parameterIndex - 1, ByteBuffer.wrap(bytes));
    }

    @Override
    public void setDate(final int parameterIndex, final Date value) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.statement = this.statement.setLocalDate(parameterIndex - 1, value.toLocalDate());
    }

    @Override
    public void setDate(final int parameterIndex, final Date date, final Calendar cal) throws SQLException {
        // silently ignore the calendar argument it is not useful for the Cassandra implementation
        setDate(parameterIndex, date);
    }

    @Override
    public void setDouble(final int parameterIndex, final double decimal) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.statement = this.statement.setDouble(parameterIndex - 1, decimal);
    }

    @Override
    public void setFloat(final int parameterIndex, final float decimal) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.statement = this.statement.setFloat(parameterIndex - 1, decimal);
    }

    @Override
    @SuppressWarnings("cast")
    public void setInt(final int parameterIndex, final int integer) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        if (DataTypeEnum.VARINT.cqlType.equals(this.statement.getPreparedStatement().getVariableDefinitions()
            .get(parameterIndex - 1).getType().asCql(false, false))) {
            this.statement = this.statement.setBigInteger(parameterIndex - 1,
                BigInteger.valueOf(Long.parseLong(String.valueOf(integer))));
        } else {
            this.statement = this.statement.setInt(parameterIndex - 1, integer);
        }
    }

    @Override
    public void setLong(final int parameterIndex, final long bigint) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.statement = this.statement.setLong(parameterIndex - 1, bigint);
    }

    @Override
    public void setNString(final int parameterIndex, final String value) throws SQLException {
        // treat like a String
        setString(parameterIndex, value);
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        // silently ignore type for cassandra... just store an empty String
        this.statement = this.statement.setToNull(parameterIndex - 1);
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType, final String typeName) throws SQLException {
        // silently ignore type and type name for cassandra... just store an empty BB
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setObject(final int parameterIndex, final Object object) throws SQLException {
        int targetType;
        if (object.getClass().equals(java.lang.Long.class)) {
            targetType = Types.BIGINT;
        } else if (object.getClass().equals(java.io.ByteArrayInputStream.class)) {
            targetType = Types.BINARY;
        } else if (object.getClass().equals(java.lang.String.class)) {
            targetType = Types.VARCHAR;
        } else if (object.getClass().equals(java.lang.Boolean.class)) {
            targetType = Types.BOOLEAN;
        } else if (object.getClass().equals(java.math.BigDecimal.class)) {
            targetType = Types.DECIMAL;
        } else if (object.getClass().equals(java.math.BigInteger.class)) {
            targetType = Types.BIGINT;
        } else if (object.getClass().equals(java.lang.Double.class)) {
            targetType = Types.DOUBLE;
        } else if (object.getClass().equals(java.lang.Float.class)) {
            targetType = Types.FLOAT;
        } else if (object.getClass().equals(java.net.Inet4Address.class)) {
            targetType = Types.OTHER;
        } else if (object.getClass().equals(java.lang.Integer.class)) {
            targetType = Types.INTEGER;
        } else if (object.getClass().equals(java.sql.Timestamp.class)) {
            targetType = Types.TIMESTAMP;
        } else if (object.getClass().equals(java.util.UUID.class)) {
            targetType = Types.ROWID;
        } else {
            targetType = Types.OTHER;
        }
        setObject(parameterIndex, object, targetType, 0);
    }

    @Override
    public void setObject(final int parameterIndex, final Object object, final int targetSqlType) throws SQLException {
        setObject(parameterIndex, object, targetSqlType, 0);
    }

    @Override
    @SuppressWarnings({"boxing", "unchecked"})
    public final void setObject(final int parameterIndex, final Object object, final int targetSqlType,
                                final int scaleOrLength) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);

        switch (targetSqlType) {
            case Types.BIGINT:
                if (object instanceof BigInteger) {
                    this.statement = this.statement.setBigInteger(parameterIndex - 1, (BigInteger) object);
                } else {
                    this.statement = this.statement.setLong(parameterIndex - 1, Long.parseLong(object.toString()));
                }
                break;
            case Types.BINARY:
                final byte[] array = new byte[((java.io.ByteArrayInputStream) object).available()];
                try {
                    ((java.io.ByteArrayInputStream) object).read(array);
                } catch (final IOException e) {
                    log.warn("Exception while setting object of BINARY type", e);
                }
                this.statement = this.statement.setByteBuffer(parameterIndex - 1, (ByteBuffer.wrap((array))));
                break;
            case Types.BOOLEAN:
                this.statement = this.statement.setBoolean(parameterIndex - 1, (Boolean) object);
                break;
            case Types.CHAR:
            case Types.CLOB:
            case Types.VARCHAR:
                this.statement = this.statement.setString(parameterIndex - 1, object.toString());
                break;
            case Types.TIMESTAMP:
                this.statement = this.statement.setInstant(parameterIndex - 1, ((Timestamp) object).toInstant());
                break;
            case Types.DECIMAL:
                this.statement = this.statement.setBigDecimal(parameterIndex - 1, (BigDecimal) object);
                break;
            case Types.DOUBLE:
                this.statement = this.statement.setDouble(parameterIndex - 1, (Double) object);
                break;
            case Types.FLOAT:
                this.statement = this.statement.setFloat(parameterIndex - 1, (Float) object);
                break;
            case Types.INTEGER:
                // If the value is an integer but the target CQL type is VARINT, cast to BigInteger.
                if (DataTypeEnum.VARINT.cqlType.equals(this.statement.getPreparedStatement().getVariableDefinitions()
                    .get(parameterIndex - 1).getType().asCql(false, false))) {
                    this.statement = this.statement.setBigInteger(parameterIndex - 1,
                        BigInteger.valueOf(Long.parseLong(object.toString())));
                } else {
                    this.statement = this.statement.setInt(parameterIndex - 1, (Integer) object);
                }
                break;
            case Types.DATE:
                this.statement = this.statement.setLocalDate(parameterIndex - 1, ((Date) object).toLocalDate());
                break;
            case Types.ROWID:
                this.statement = this.statement.setUuid(parameterIndex - 1, (java.util.UUID) object);
                break;
            case Types.OTHER:
                if (object.getClass().equals(TupleValue.class)) {
                    this.statement = this.statement.setTupleValue(parameterIndex - 1, (TupleValue) object);
                }
                if (object.getClass().equals(java.util.UUID.class)) {
                    this.statement = this.statement.setUuid(parameterIndex - 1, (java.util.UUID) object);
                }
                if (object.getClass().equals(java.net.InetAddress.class) || object.getClass().equals(java.net.Inet4Address.class)) {
                    assert object instanceof InetAddress;
                    this.statement = this.statement.setInetAddress(parameterIndex - 1, (InetAddress) object);
                } else if (List.class.isAssignableFrom(object.getClass())) {
                    final List handledList = handleAsList(object.getClass(), object);
                    final DefaultListType listType = (DefaultListType) this.statement.getPreparedStatement()
                        .getVariableDefinitions().get(parameterIndex - 1).getType();
                    final Class<?> itemsClass = DataTypeEnum.fromCqlTypeName(listType.getElementType()
                        .asCql(false, false)).javaType;
                    this.statement = this.statement.setList(parameterIndex - 1, handledList, itemsClass);
                } else if (Set.class.isAssignableFrom(object.getClass())) {
                    final Set handledSet = handleAsSet(object.getClass(), object);
                    final DefaultSetType setType = (DefaultSetType) this.statement.getPreparedStatement()
                        .getVariableDefinitions().get(parameterIndex - 1).getType();
                    final Class<?> itemsClass = DataTypeEnum.fromCqlTypeName(setType.getElementType()
                        .asCql(false, false)).javaType;
                    this.statement = this.statement.setSet(parameterIndex - 1, handledSet, itemsClass);
                } else if (Map.class.isAssignableFrom(object.getClass())) {
                    final Map handledMap = handleAsMap(object.getClass(), object);
                    final DefaultMapType mapType = (DefaultMapType) this.statement.getPreparedStatement()
                        .getVariableDefinitions().get(parameterIndex - 1).getType();
                    final Class<?> keysClass = DataTypeEnum.fromCqlTypeName(mapType.getKeyType()
                        .asCql(false, false)).javaType;
                    final Class<?> valuesClass = DataTypeEnum.fromCqlTypeName(mapType.getValueType()
                        .asCql(false, false)).javaType;
                    this.statement = this.statement.setMap(parameterIndex - 1, handledMap, keysClass, valuesClass);
                }

                break;
        }
    }

    @Override
    public void setRowId(final int parameterIndex, final RowId value) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.statement.setToNull(parameterIndex - 1);
    }

    @Override
    public void setShort(final int parameterIndex, final short smallint) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.statement.setInt(parameterIndex - 1, smallint);
    }

    @Override
    public void setString(final int parameterIndex, final String value) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        this.statement = this.statement.setString(parameterIndex - 1, value);
    }

    @Override
    @SuppressWarnings("boxing")
    public void setTime(final int parameterIndex, final Time value) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        // time type data is handled as an 8 byte Long value of milliseconds since the epoch
        bindValues.put(parameterIndex,
            value == null ? null : JdbcLong.instance.decompose(value.getTime()));
    }

    @Override
    public void setTime(final int parameterIndex, final Time value, final Calendar cal) throws SQLException {
        // silently ignore the calendar argument it is not useful for the Cassandra implementation
        setTime(parameterIndex, value);
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp value) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        // timestamp type data is handled as an 8 byte Long value of milliseconds since the epoch. Nanos are not
        // supported and are ignored.
        this.statement = this.statement.setInstant(parameterIndex - 1, value.toInstant());
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp value, final Calendar cal) throws SQLException {
        // silently ignore the calendar argument it is not useful for the Cassandra implementation
        setTimestamp(parameterIndex, value);
    }

    @Override
    public void setURL(final int parameterIndex, final URL value) throws SQLException {
        checkNotClosed();
        checkIndex(parameterIndex);
        // URl type data is handled as an string
        final String url = value.toString();
        setString(parameterIndex, url);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkNotClosed();
        return currentResultSet;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> List handleAsList(final Class<?> objectClass, final Object object) {
        if (!List.class.isAssignableFrom(objectClass)) {
            return null;
        }
        return (List<T>) object.getClass().cast(object);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> Set handleAsSet(final Class<?> objectClass, final Object object) {
        if (!Set.class.isAssignableFrom(objectClass)) {
            return null;
        }
        return (Set<T>) object.getClass().cast(object);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <K, V> HashMap handleAsMap(final Class<?> objectClass, final Object object) {
        if (!Map.class.isAssignableFrom(objectClass)) {
            return null;
        }
        return (HashMap<K, V>) object.getClass().cast(object);
    }

    @SuppressWarnings("rawtypes")
    private static Class<?> getCollectionElementType(final Object maybeCollection) {
        final Collection trial = (Collection) maybeCollection;
        if (trial.isEmpty()) {
            return trial.getClass();
        }
        return trial.iterator().next().getClass();
    }

    @SuppressWarnings({"unused", "rawtypes"})
    private static Class<?> getKeyElementType(final Object maybeMap) {
        return getCollectionElementType(((Map) maybeMap).keySet());
    }

    @SuppressWarnings({"unused", "rawtypes"})
    private static Class<?> getValueElementType(final Object maybeMap) {
        return getCollectionElementType(((Map) maybeMap).values());
    }

    @SuppressWarnings("boxing")
    @Override
    public void setBlob(final int parameterIndex, final Blob value) throws SQLException {
        final InputStream in = value.getBinaryStream();
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

        this.statement = this.statement.setByteBuffer(parameterIndex - 1, (ByteBuffer.wrap((buffer.toByteArray()))));
    }

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

}
