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
import com.ing.data.cassandra.jdbc.types.DataTypeEnum;
import org.apache.commons.lang3.StringUtils;

import jakarta.annotation.Nonnull;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Wrapper;
import java.util.Map;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NO_INTERFACE;

/**
 * Provides a default implementation (returning a {@link SQLFeatureNotSupportedException}) to hold the unimplemented
 * methods of {@link java.sql.ResultSet} interface. It also provides helper methods for CQL type management in the
 * implementation of {@link java.sql.ResultSet} interface.
 */
@SuppressWarnings("unused")
abstract class AbstractResultSet implements Wrapper {

    /**
     * Checks whether the {@code i}th column has the given CQL data type.
     *
     * @param columnIndex The column index (first column is 1).
     * @param type        The data type to check.
     * @return {@code true} if the column CQL data type is the given one, {@code false} otherwise.
     * @throws SQLException when the CQL type cannot be determined for the given column.
     */
    boolean isCqlType(final int columnIndex, @Nonnull final DataTypeEnum type) throws SQLException {
        final String columnType = StringUtils.substringBefore(DataTypeEnum.cqlName(getCqlDataType(columnIndex)), "<");
        return type.cqlType.equalsIgnoreCase(columnType);
    }

    /**
     * Checks whether the column with the given name has the given CQL data type.
     *
     * @param columnLabel The column name.
     * @param type        The data type to check.
     * @return {@code true} if the column CQL data type is the given one, {@code false} otherwise.
     * @throws SQLException when the CQL type cannot be determined for the given column.
     */
    boolean isCqlType(final String columnLabel, @Nonnull final DataTypeEnum type) throws SQLException {
        final String columnType = StringUtils.substringBefore(DataTypeEnum.cqlName(getCqlDataType(columnLabel)), "<");
        return type.cqlType.equalsIgnoreCase(columnType);
    }

    /**
     * Gets the CQL type of the {@code i}th column.
     *
     * @param columnIndex The column index (first column is 1).
     * @return The CQL data type of the column.
     * @throws SQLException when the CQL type cannot be determined for the given column.
     */
    abstract DataType getCqlDataType(int columnIndex) throws SQLException;

    /**
     * Gets the CQL type of the column with the given name.
     *
     * @param columnLabel The column name.
     * @return The CQL data type of the column.
     * @throws SQLException when the CQL type cannot be determined for the given column.
     */
    abstract DataType getCqlDataType(String columnLabel) throws SQLException;

    public boolean absolute(final int row) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public Array getArray(final int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public Array getArray(final String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public InputStream getAsciiStream(final int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public InputStream getAsciiStream(final String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public Reader getCharacterStream(final int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public Reader getCharacterStream(final String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public Clob getClob(final int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public Clob getClob(final String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public Reader getNCharacterStream(final int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public Reader getNCharacterStream(final String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public NClob getNClob(final int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public NClob getNClob(final String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public String getNString(final int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public String getNString(final String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public Object getObject(final String columnLabel, final Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public Ref getRef(final int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public Ref getRef(final String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public SQLXML getSQLXML(final int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public SQLXML getSQLXML(final String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public InputStream getUnicodeStream(final int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public InputStream getUnicodeStream(final String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean relative(final int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    /*
     * All the update methods are unsupported, it requires a separate statement in Cassandra.
     */

    public void updateArray(final int columnIndex, final Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateArray(final String columnLabel, final Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateAsciiStream(final int columnLabel, final InputStream x, final int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateAsciiStream(final int columnIndex, final InputStream x, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateAsciiStream(final String columnLabel, final InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateAsciiStream(final String columnLabel, final InputStream x, final int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateAsciiStream(final String columnLabel, final InputStream x, final long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBigDecimal(final String columnLabel, final BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBinaryStream(final int columnIndex, final InputStream length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBinaryStream(final int columnIndex, final InputStream x, final int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBinaryStream(final int columnIndex, final InputStream x, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBinaryStream(final String columnLabel, final InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBinaryStream(final String columnLabel, final InputStream x, final int length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBinaryStream(final String columnLabel, final InputStream x, final long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBlob(final int columnIndex, final Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBlob(final int columnIndex, final InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBlob(final int columnIndex, final InputStream x, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBlob(final String columnLabel, final Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBlob(final String columnLabel, final InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBlob(final String columnLabel, final InputStream x, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBoolean(final int columnIndex, final boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBoolean(final String columnLabel, final boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateByte(final int columnIndex, final byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateByte(final String columnLabel, final byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBytes(final int columnIndex, final byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateBytes(final String columnLabel, final byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateCharacterStream(final int columnIndex, final Reader x, final int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateCharacterStream(final String columnLabel, final Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateCharacterStream(final String columnLabel, final Reader x, final int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateCharacterStream(final String columnLabel, final Reader x, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateClob(final int columnIndex, final Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateClob(final int columnIndex, final Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateClob(final int columnIndex, final Reader x, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateClob(final String columnLabel, final Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateClob(final String columnLabel, final Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateClob(final String columnLabel, final Reader x, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateDate(final int columnIndex, final Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateDate(final String columnLabel, final Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateDouble(final int columnIndex, final double x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateDouble(final String columnLabel, final double x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateFloat(final int columnIndex, final float x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateFloat(final String columnLabel, final float x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateInt(final int columnIndex, final int x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateInt(final String columnLabel, final int x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateLong(final int columnIndex, final long x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateLong(final String columnLabel, final long x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNCharacterStream(final String columnLabel, final Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNCharacterStream(final String columnLabel, final Reader x, final long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNClob(final int columnIndex, final NClob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNClob(final int columnIndex, final Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNClob(final int columnIndex, final Reader x, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNClob(final String columnLabel, final NClob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNClob(final String columnLabel, final Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNClob(final String columnLabel, final Reader x, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNString(final int columnIndex, final String x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNString(final String columnLabel, final String x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNull(final int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateNull(final String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateObject(final int columnIndex, final Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateObject(final int columnIndex, final Object x, final int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateObject(final String columnLabel, final Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateObject(final String columnLabel, final Object x, final int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateRef(final int columnIndex, final Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateRef(final String columnLabel, final Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateRowId(final int columnIndex, final RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateRowId(final String columnLabel, final RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateShort(final int columnIndex, final short x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateShort(final String columnLabel, final short x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateSQLXML(final int columnIndex, final SQLXML x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateSQLXML(final String columnLabel, final SQLXML x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateString(final int columnIndex, final String x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateString(final String columnLabel, final String x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateTime(final int columnIndex, final Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateTime(final String columnLabel, final Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void updateTimestamp(final String columnLabel, final Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
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
