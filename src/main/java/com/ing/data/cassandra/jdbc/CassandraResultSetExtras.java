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

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extension of {@link ResultSet} interface providing additional methods specific to Cassandra result sets.
 */
public interface CassandraResultSetExtras extends ResultSet {

    /**
     * Retrieves the value of the designated column in the current row of this {@code ResultSet} object as a
     * {@link BigInteger}.
     *
     * @param columnIndex The column index (the first column is 1).
     * @return The column value. If the value is SQL {@code NULL}, it should return {@code null}.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs or this method is called
     * on a closed result set.
     */
    BigInteger getBigInteger(int columnIndex) throws SQLException;

    /**
     * Retrieves the value of the designated column in the current row of this {@code ResultSet} object as a
     * {@link BigInteger}.
     *
     * @param columnLabel The label for the column specified with the SQL AS clause. If the SQL AS clause was not
     *                    specified, then the label is the name of the column.
     * @return The column value. If the value is SQL {@code NULL}, it should return {@code null}.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs or this method is called
     * on a closed result set.
     */
    BigInteger getBigInteger(String columnLabel) throws SQLException;

    /**
     * Retrieves the value of the designated column in the current row of this {@code ResultSet} object as a
     * {@link List}.
     *
     * @param columnIndex The column index (the first column is 1).
     * @return The column value. If the value is SQL {@code NULL}, it should return an empty collection or {@code null},
     * depending on the driver implementation.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs or this method is called
     * on a closed result set.
     */
    List<?> getList(int columnIndex) throws SQLException;

    /**
     * Retrieves the value of the designated column in the current row of this {@code ResultSet} object as a
     * {@link List}.
     *
     * @param columnLabel The label for the column specified with the SQL AS clause. If the SQL AS clause was not
     *                    specified, then the label is the name of the column.
     * @return The column value. If the value is SQL {@code NULL}, it should return an empty collection or {@code null},
     * depending on the driver implementation.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs or this method is called
     * on a closed result set.
     */
    List<?> getList(String columnLabel) throws SQLException;

    /**
     * Retrieves the value of the designated column in the current row of this {@code ResultSet} object as a
     * {@link Set}.
     *
     * @param columnIndex The column index (the first column is 1).
     * @return The column value. If the value is SQL {@code NULL}, it should return an empty collection or {@code null},
     * depending on the driver implementation.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs or this method is called
     * on a closed result set.
     */
    Set<?> getSet(int columnIndex) throws SQLException;

    /**
     * Retrieves the value of the designated column in the current row of this {@code ResultSet} object as a
     * {@link Set}.
     *
     * @param columnLabel The label for the column specified with the SQL AS clause. If the SQL AS clause was not
     *                    specified, then the label is the name of the column.
     * @return The column value. If the value is SQL {@code NULL}, it should return an empty collection or {@code null},
     * depending on the driver implementation.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs or this method is called
     * on a closed result set.
     */
    Set<?> getSet(String columnLabel) throws SQLException;

    /**
     * Retrieves the value of the designated column in the current row of this {@code ResultSet} object as a
     * {@link Map}.
     *
     * @param columnIndex The column index (the first column is 1).
     * @return The column value. If the value is SQL {@code NULL}, it should return an empty collection or {@code null},
     * depending on the driver implementation.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs or this method is called
     * on a closed result set.
     */
    Map<?, ?> getMap(int columnIndex) throws SQLException;

    /**
     * Retrieves the value of the designated column in the current row of this {@code ResultSet} object as a
     * {@link Map}.
     *
     * @param columnLabel The label for the column specified with the SQL AS clause. If the SQL AS clause was not
     *                    specified, then the label is the name of the column.
     * @return The column value. If the value is SQL {@code NULL}, it should return an empty collection or {@code null},
     * depending on the driver implementation.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs or this method is called
     * on a closed result set.
     */
    Map<?, ?> getMap(String columnLabel) throws SQLException;

    /**
     * Retrieves the value of the designated column in the current row of this {@code ResultSet} object as a
     * {@link CqlDuration}.
     *
     * @param columnIndex The column index (the first column is 1).
     * @return The column value. If the value is SQL {@code NULL}, it should return {@code null}.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs or this method is called
     * on a closed result set.
     */
    CqlDuration getDuration(int columnIndex) throws SQLException;

    /**
     * Retrieves the value of the designated column in the current row of this {@code ResultSet} object as a
     * {@link CqlDuration}.
     *
     * @param columnLabel The label for the column specified with the SQL AS clause. If the SQL AS clause was not
     *                    specified, then the label is the name of the column.
     * @return The column value. If the value is SQL {@code NULL}, it should return {@code null}.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs or this method is called
     * on a closed result set.
     */
    CqlDuration getDuration(String columnLabel) throws SQLException;

}
