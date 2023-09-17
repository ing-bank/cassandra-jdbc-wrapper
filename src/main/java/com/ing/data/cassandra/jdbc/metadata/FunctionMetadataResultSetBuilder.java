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

package com.ing.data.cassandra.jdbc.metadata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.ing.data.cassandra.jdbc.CassandraMetadataResultSet;
import com.ing.data.cassandra.jdbc.CassandraStatement;
import com.ing.data.cassandra.jdbc.types.AbstractJdbcType;
import org.apache.commons.lang3.StringUtils;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.ing.data.cassandra.jdbc.types.TypesMap.getTypeForComparator;
import static java.sql.DatabaseMetaData.functionColumnIn;
import static java.sql.DatabaseMetaData.functionReturn;
import static java.sql.DatabaseMetaData.typeNullable;

/**
 * Utility class building metadata result sets ({@link CassandraMetadataResultSet} objects) related to functions.
 */
public class FunctionMetadataResultSetBuilder extends AbstractMetadataResultSetBuilder {

    /**
     * Constructor.
     *
     * @param statement The statement.
     * @throws SQLException if a database access error occurs or this statement is closed.
     */
    public FunctionMetadataResultSetBuilder(final CassandraStatement statement) throws SQLException {
        super(statement);
    }

    /**
     * Builds a valid result set of the system and user functions available in the given catalog (Cassandra cluster).
     * This method is used to implement the method {@link DatabaseMetaData#getFunctions(String, String, String)}.
     * <p>
     * Only system and user function descriptions matching the schema and function name criteria are returned. They are
     * ordered by {@code FUNCTION_CAT}, {@code FUNCTION_SCHEM}, {@code FUNCTION_NAME} and {@code SPECIFIC_NAME}.
     * </p>
     * <p>
     * The columns of this result set are:
     *     <ol>
     *         <li><b>FUNCTION_CAT</b> String => function catalog, may be {@code null}: here is the Cassandra cluster
     *         name (if available).</li>
     *         <li><b>FUNCTION_SCHEM</b> String => function schema, may be {@code null}: here is the keyspace the table
     *         is member of.</li>
     *         <li><b>FUNCTION_NAME</b> String => function name. This is the name used to invoke the function.</li>
     *         <li><b>REMARKS</b> String => explanatory comment on the function (always empty, Cassandra does not
     *         allow to describe functions with a comment).</li>
     *         <li><b>FUNCTION_TYPE</b> short => kind of function:
     *             <ul>
     *                 <li>{@link DatabaseMetaData#functionResultUnknown} - cannot determine if a return value or table
     *                 will be returned</li>
     *                 <li>{@link DatabaseMetaData#functionNoTable} - does not return a table (Cassandra user-defined
     *                 functions only return CQL types, so never a table)</li>
     *                 <li>{@link DatabaseMetaData#functionReturnsTable} - returns a table</li>
     *             </ul>
     *         </li>
     *         <li><b>SPECIFIC_NAME</b> String => the name which uniquely identifies this function within its schema.
     *         This is a user specified, or DBMS generated, name that may be different then the {@code FUNCTION_NAME}
     *         for example with overload functions.</li>
     *     </ol>
     * </p>
     * <p>
     * A user may not have permission to execute any of the functions that are returned by {@code getFunctions}.
     * </p>
     *
     * @param schemaPattern         A schema name pattern. It must match the schema name as it is stored in the
     *                              database; {@code ""} retrieves those without a schema and {@code null} means that
     *                              the schema name should not be used to narrow down the search.
     * @param functionNamePattern   A function name pattern; must match the function name as it is stored in the
     *                              database.
     * @return A valid result set for implementation of {@link DatabaseMetaData#getFunctions(String, String, String)}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */

    public CassandraMetadataResultSet buildFunctions(final String schemaPattern,
                                                     final String functionNamePattern) throws SQLException {
        final String catalog = this.connection.getCatalog();
        final ArrayList<MetadataRow> functionsRows = new ArrayList<>();

        filterBySchemaNamePattern(schemaPattern, keyspaceMetadata ->
            filterByFunctionNamePattern(functionNamePattern, keyspaceMetadata,
                (functionSignature, functionMetadata) -> {
                    final MetadataRow row = new MetadataRow()
                        .addEntry(FUNCTION_CATALOG, catalog)
                        .addEntry(FUNCTION_SCHEMA, keyspaceMetadata.getName().asInternal())
                        .addEntry(FUNCTION_NAME, functionSignature.getName().asInternal())
                        .addEntry(REMARKS, StringUtils.EMPTY)
                        .addEntry(FUNCTION_TYPE, String.valueOf(DatabaseMetaData.functionNoTable))
                        .addEntry(SPECIFIC_NAME, functionSignature.getName().asInternal());
                    functionsRows.add(row);
                }), null);

        // Results should all have the same FUNCTION_CAT, so just sort them by FUNCTION_SCHEM then FUNCTION_NAME (since
        // here SPECIFIC_NAME is equal to FUNCTION_NAME).
        functionsRows.sort(Comparator.comparing(row -> ((MetadataRow) row).getString(FUNCTION_SCHEMA))
            .thenComparing(row -> ((MetadataRow) row).getString(FUNCTION_NAME)));
        return CassandraMetadataResultSet.buildFrom(this.statement, new MetadataResultSet().setRows(functionsRows));
    }

    /**
     * Builds a valid result set of the given catalog's system or user function parameters and return type.
     * This method is used to implement the method
     * {@link DatabaseMetaData#getFunctionColumns(String, String, String, String)}.
     * <p>
     * Only descriptions matching the schema, function and parameter name criteria are returned. They are ordered by
     * {@code FUNCTION_CAT}, {@code FUNCTION_SCHEM}, {@code FUNCTION_NAME} and {@code SPECIFIC_NAME}. Within this, the
     * return value, if any, is first. Next are the parameter descriptions in call order. The column descriptions
     * follow in column number order.
     * </p>
     * <p>
     * The columns of this result set are:
     *     <ol>
     *         <li><b>FUNCTION_CAT</b> String => function catalog, may be {@code null}: here is the Cassandra cluster
     *         name (if available).</li>
     *         <li><b>FUNCTION_SCHEM</b> String => function schema, may be {@code null}: here is the keyspace the table
     *         is member of.</li>
     *         <li><b>FUNCTION_NAME</b> String => function name. This is the name used to invoke the function.</li>
     *         <li><b>COLUMN_NAME</b> String => column/parameter name.</li>
     *         <li><b>COLUMN_TYPE</b> short => kind of column/parameter:
     *             <ul>
     *                 <li>{@link DatabaseMetaData#functionColumnUnknown} - unknown type</li>
     *                 <li>{@link DatabaseMetaData#functionColumnIn} - {@code IN} parameter</li>
     *                 <li>{@link DatabaseMetaData#functionColumnInOut} - {@code INOUT} parameter</li>
     *                 <li>{@link DatabaseMetaData#functionColumnOut} - {@code OUT} parameter</li>
     *                 <li>{@link DatabaseMetaData#functionReturn} - function return value</li>
     *                 <li>{@link DatabaseMetaData#functionColumnResult} - indicates that the parameter or column is a
     *                 column in the {@code ResultSet}</li>
     *             </ul>
     *         </li>
     *         <li><b>DATA_TYPE</b> int => SQL data type from {@link Types}.</li>
     *         <li><b>TYPE_NAME</b> String => SQL type name, for a UDT type the type name is fully qualified.</li>
     *         <li><b>PRECISION</b> int => maximum precision.</li>
     *         <li><b>LENGTH</b> int => length in bytes of data.</li>
     *         <li><b>SCALE</b> int => scale, {@code null} is returned for data types where SCALE is not
     *         applicable.</li>
     *         <li><b>RADIX</b> short => precision radix.</li>
     *         <li><b>NULLABLE</b> short => can you use {@code NULL} for this type:
     *              <ul>
     *                  <li>{@link DatabaseMetaData#typeNoNulls} - does not allow {@code NULL} values</li>
     *                  <li>{@link DatabaseMetaData#typeNullable} - allows {@code NULL} values</li>
     *                  <li>{@link DatabaseMetaData#typeNullableUnknown} - nullability unknown</li>
     *              </ul>
     *         </li>
     *         <li><b>REMARKS</b> String => comment describing column/parameter (always empty, Cassandra does not
     *         allow to describe columns with a comment).</li>
     *         <li><b>CHAR_OCTET_LENGTH</b> int => the maximum length of binary and character based parameters or
     *         columns. For any other datatype the returned value is a {@code NULL}.</li>
     *         <li><b>ORDINAL_POSITION</b> int => the ordinal position, starting from 1, for the input and output
     *         parameters. A value of 0 is returned if this row describes the function's return value. For result set
     *         columns, it is the ordinal position of the column in the result set starting from 1.</li>
     *         <li><b>IS_NULLABLE</b> String => "YES" if a parameter or column accepts {@code NULL} values, "NO"
     *         if not and empty if the nullability is unknown.</li>
     *         <li><b>SPECIFIC_NAME</b> String => the name which uniquely identifies this function within its schema.
     *         This is a user specified, or DBMS generated, name that may be different then the {@code FUNCTION_NAME}
     *         for example with overload functions.</li>
     *     </ol>
     * </p>
     * <p>
     * The {@code PRECISION} column represents the maximum column size that the server supports for the given datatype.
     * For numeric data, this is the maximum precision. For character data, this is the length in characters. For
     * datetime data types, this is the length in characters of the {@code String} representation (assuming the maximum
     * allowed precision of the fractional seconds component). For binary data, this is the length in bytes.
     * For the {@code ROWID} datatype (not supported by Cassandra), this is the length in bytes. The value {@code null}
     * is returned for data types where the column size is not applicable.
     * </p>
     *
     * @param schemaPattern         A schema name pattern. It must match the schema name as it is stored in the
     *                              database; {@code ""} retrieves those without a schema and {@code null} means that
     *                              the schema name should not be used to narrow down the search.
     * @param functionNamePattern   A function name pattern; must match the function name as it is stored in the
     *                              database.
     * @param columnNamePattern     A parameter name pattern; must match the parameter or column name as it is stored
     *                              in the database.
     * @return A valid result set for implementation of
     * {@link DatabaseMetaData#getFunctionColumns(String, String, String, String)}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet buildFunctionColumns(final String schemaPattern,
                                                           final String functionNamePattern,
                                                           final String columnNamePattern) throws SQLException {
        final String catalog = this.connection.getCatalog();
        final ArrayList<MetadataRow> functionParamsRows = new ArrayList<>();

        filterBySchemaNamePattern(schemaPattern, keyspaceMetadata ->
            filterByFunctionNamePattern(functionNamePattern, keyspaceMetadata,
                (functionSignature, functionMetadata) -> {
                    // Function return type.
                    final AbstractJdbcType<?> returnJdbcType =
                        getTypeForComparator(functionMetadata.getReturnType().asCql(false, true));
                    final MetadataRow row = new MetadataRow()
                        .addEntry(FUNCTION_CATALOG, catalog)
                        .addEntry(FUNCTION_SCHEMA, keyspaceMetadata.getName().asInternal())
                        .addEntry(FUNCTION_NAME, functionSignature.getName().asInternal())
                        .addEntry(COLUMN_NAME, StringUtils.EMPTY)
                        .addEntry(COLUMN_TYPE, String.valueOf(functionReturn))
                        .addEntry(DATA_TYPE, String.valueOf(returnJdbcType.getJdbcType()))
                        .addEntry(TYPE_NAME, functionMetadata.getReturnType().toString())
                        .addEntry(PRECISION, String.valueOf(returnJdbcType.getPrecision(null)))
                        .addEntry(LENGTH, String.valueOf(Integer.MAX_VALUE))
                        .addEntry(SCALE, String.valueOf(returnJdbcType.getScale(null)))
                        .addEntry(RADIX, String.valueOf(returnJdbcType.getPrecision(null)))
                        .addEntry(NULLABLE, String.valueOf(typeNullable))
                        .addEntry(REMARKS, StringUtils.EMPTY)
                        .addEntry(CHAR_OCTET_LENGTH, null)
                        .addEntry(ORDINAL_POSITION, "0")
                        .addEntry(IS_NULLABLE, YES_VALUE)
                        .addEntry(SPECIFIC_NAME, functionSignature.getName().asInternal());
                    functionParamsRows.add(row);

                    // Function input parameters.
                    final List<CqlIdentifier> paramNames = functionMetadata.getParameterNames();
                    for (int i = 0; i < paramNames.size(); i++) {
                        if (columnNamePattern == null
                            || matchesPattern(columnNamePattern, paramNames.get(i).asInternal())) {
                            final AbstractJdbcType<?> paramJdbcType = getTypeForComparator(
                                functionSignature.getParameterTypes().get(i).asCql(false, true));
                            final MetadataRow paramRow = new MetadataRow()
                                .addEntry(FUNCTION_CATALOG, catalog)
                                .addEntry(FUNCTION_SCHEMA, keyspaceMetadata.getName().asInternal())
                                .addEntry(FUNCTION_NAME, functionSignature.getName().asInternal())
                                .addEntry(COLUMN_NAME, paramNames.get(i).asInternal())
                                .addEntry(COLUMN_TYPE, String.valueOf(functionColumnIn))
                                .addEntry(DATA_TYPE, String.valueOf(paramJdbcType.getJdbcType()))
                                .addEntry(TYPE_NAME, functionSignature.getParameterTypes().get(i).toString())
                                .addEntry(PRECISION, String.valueOf(paramJdbcType.getPrecision(null)))
                                .addEntry(LENGTH, String.valueOf(Integer.MAX_VALUE))
                                .addEntry(SCALE, String.valueOf(paramJdbcType.getScale(null)))
                                .addEntry(RADIX, String.valueOf(paramJdbcType.getPrecision(null)))
                                .addEntry(NULLABLE, String.valueOf(typeNullable))
                                .addEntry(REMARKS, StringUtils.EMPTY)
                                .addEntry(CHAR_OCTET_LENGTH, null)
                                .addEntry(ORDINAL_POSITION, String.valueOf(i + 1))
                                .addEntry(IS_NULLABLE, YES_VALUE)
                                .addEntry(SPECIFIC_NAME, functionSignature.getName().asInternal());
                            functionParamsRows.add(paramRow);
                        }
                    }
                }), null);

        // Results should all have the same FUNCTION_CAT, so just sort them by FUNCTION_SCHEM then FUNCTION_NAME (since
        // here SPECIFIC_NAME is equal to FUNCTION_NAME), and finally by ORDINAL_POSITION.
        functionParamsRows.sort(Comparator.comparing(row -> ((MetadataRow) row).getString(FUNCTION_SCHEMA))
            .thenComparing(row -> ((MetadataRow) row).getString(FUNCTION_NAME))
            .thenComparing(row -> ((MetadataRow) row).getString(SPECIFIC_NAME))
            .thenComparing(row -> Integer.valueOf(((MetadataRow) row).getString(ORDINAL_POSITION))));
        return CassandraMetadataResultSet.buildFrom(this.statement,
            new MetadataResultSet().setRows(functionParamsRows));
    }

}
