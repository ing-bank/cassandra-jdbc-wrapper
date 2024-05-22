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

import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;

public class ListResultSet extends AbstractResultSet implements ResultSet {
    private final List<Object[]> data;
    private final ColumnMetaData[] columns;
    private int currentRow = -1;
    private boolean isClosed = false;

    public ListResultSet(List<Object[]> data, ColumnMetaData[] columns) {
        this.data = data;
        this.columns = columns;
    }

    public ListResultSet(Object value, ColumnMetaData columnName) {
        this.data = new ArrayList<>();
        this.data.add(new Object[]{value});
        this.columns = new ColumnMetaData[]{columnName};
    }

    @Override
    public boolean next() {
        if (data == null) {
            return false;
        }
        if (currentRow < data.size() - 1) {
            currentRow++;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        checkClosed();
        this.isClosed = true;
    }

    private void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("Result set already closed");
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    DataType getCqlDataType(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    DataType getCqlDataType(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public boolean wasNull() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        if (currentRow >= data.size()) {
            throw new SQLException("ResultSet exhausted, request currentRow = " + currentRow);
        }
        int adjustedColumnIndex = columnIndex - 1;
        if (adjustedColumnIndex >= data.get(currentRow).length) {
            throw new SQLException("Column index does not exist: " + columnIndex);
        }
        final Object val = data.get(currentRow)[adjustedColumnIndex];
        return val != null ? val.toString() : null;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkClosed();
        return Boolean.parseBoolean(getString(columnIndex));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkClosed();
        return Integer.parseInt(getString(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkClosed();
        return Long.parseLong(getString(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkClosed();
        return Float.parseFloat(getString(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkClosed();
        return Double.parseDouble(getString(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        checkClosed();
        int index = -1;
        if (columns == null) {
            throw new SQLException("Use of columnLabel requires setColumnNames to be called first.");
        }
        for (int i = 0; i < columns.length; i++) {
            if (columnLabel.equals(columns[i].name)) {
                index = i;
                break;
            }
        }
        if (index == -1) {
            throw new SQLException("Column " + columnLabel + " doesn't exist in this ResultSet");
        }
        return getString(index + 1);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        checkClosed();
        return Double.parseDouble(getString(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getString(columnLabel).getBytes();
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new ListResultSetMetaData(columns);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (currentRow >= data.size()) {
            throw new SQLException("ResultSet exhausted, request currentRow = " + currentRow);
        }
        int adjustedColumnIndex = columnIndex - 1;
        if (adjustedColumnIndex >= data.get(currentRow).length) {
            throw new SQLException("Column index does not exist: " + columnIndex);
        }
        return data.get(currentRow)[adjustedColumnIndex];
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].name.equals(columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("No such column " + columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public int getRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement getStatement() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public static class ListResultSetMetaData implements ResultSetMetaData {
        private final ColumnMetaData[] columnMetaData;

        ListResultSetMetaData(ColumnMetaData[] columnMetaData) {
            this.columnMetaData = columnMetaData;
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public int getColumnCount() {
            return this.columnMetaData.length;
        }

        @Override
        public boolean isAutoIncrement(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public boolean isCaseSensitive(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public boolean isSearchable(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public boolean isCurrency(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public int isNullable(int column) {
            return ResultSetMetaData.columnNullable;
        }

        @Override
        public boolean isSigned(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public int getColumnDisplaySize(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public String getColumnLabel(int column) {
            return columnMetaData[column - 1].name;
        }

        @Override
        public String getColumnName(int column) {
            return columnMetaData[column - 1].name;
        }

        @Override
        public String getSchemaName(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public int getPrecision(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public int getScale(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public String getTableName(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public String getCatalogName(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public int getColumnType(int column) {
            return columnMetaData[column - 1].getJavaType();
        }

        @Override
        public String getColumnTypeName(int column) {
            return columnMetaData[column - 1].typeName;
        }

        @Override
        public boolean isReadOnly(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public boolean isWritable(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public boolean isDefinitelyWritable(int column) throws SQLException {
            throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
        }

        @Override
        public String getColumnClassName(int column) {
            return columnMetaData[column - 1].getClassName();
        }
    }

    public static class ColumnMetaData {
        private final String name;
        private final String typeName;
        private final int type;
        private final String className;

        ColumnMetaData(String name, String typeName, int type, String className) {
            this.name = name;
            this.typeName = typeName;
            this.type = type;
            this.className = className;
        }

        int getJavaType() {
            return type;
        }

        String getClassName() {
            return className;
        }
    }
}
