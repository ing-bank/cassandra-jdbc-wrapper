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

import java.io.InputStream;
import java.io.Reader;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Wrapper;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NO_INTERFACE;

/**
 * Provides a default implementation (returning a {@link SQLFeatureNotSupportedException}) to hold the unimplemented
 * methods of {@link java.sql.Statement} and {@link java.sql.PreparedStatement} interfaces.
 */
@SuppressWarnings("unused")
abstract class AbstractStatement implements Wrapper {

    /*
     * From the Statement implementation.
     */

    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean execute(final String cql, final int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean execute(final String cql, final String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public int executeUpdate(final String cql, final int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public int executeUpdate(final String cql, final String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setCursorName(final String name) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    /*
     * From the PreparedStatement implementation.
     */

    public void setAsciiStream(final int parameterIndex, final InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setAsciiStream(final int parameterIndex,
                               final InputStream x,
                               final int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setAsciiStream(final int parameterIndex,
                               final InputStream x,
                               final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setBinaryStream(final int parameterIndex, final InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setBinaryStream(final int parameterIndex,
                                final InputStream x,
                                final int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setBinaryStream(final int parameterIndex,
                                final InputStream x,
                                final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setBlob(final int parameterIndex,
                        final InputStream inputStream,
                        final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setCharacterStream(final int parameterIndex, final Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setCharacterStream(final int parameterIndex,
                                   final Reader reader,
                                   final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setClob(final int parameterIndex, final Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setClob(final int parameterIndex, final Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setClob(final int parameterIndex,
                        final Reader reader,
                        final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setNCharacterStream(final int parameterIndex, final Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setNCharacterStream(final int parameterIndex,
                                    final Reader value,
                                    final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setNClob(final int parameterIndex, final NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setNClob(final int parameterIndex, final Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setNClob(final int parameterIndex,
                         final Reader reader,
                         final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setRef(final int parameterIndex, final Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setSQLXML(final int parameterIndex, final SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Deprecated
    public void setUnicodeStream(final int parameterIndex,
                                 final InputStream x,
                                 final int length) throws SQLException {
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
