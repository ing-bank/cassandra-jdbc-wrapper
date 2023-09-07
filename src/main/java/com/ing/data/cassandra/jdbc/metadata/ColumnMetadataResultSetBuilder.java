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

import com.ing.data.cassandra.jdbc.CassandraMetadataResultSet;
import com.ing.data.cassandra.jdbc.CassandraStatement;
import com.ing.data.cassandra.jdbc.types.AbstractJdbcType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ing.data.cassandra.jdbc.types.AbstractJdbcType.DEFAULT_PRECISION;
import static com.ing.data.cassandra.jdbc.types.TypesMap.getTypeForComparator;

/**
 * Utility class building metadata result sets ({@link CassandraMetadataResultSet} objects) related to columns.
 */
public class ColumnMetadataResultSetBuilder extends AbstractMetadataResultSetBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnMetadataResultSetBuilder.class);

    /**
     * Constructor.
     *
     * @param statement The statement.
     * @throws SQLException if a database access error occurs or this statement is closed.
     */
    public ColumnMetadataResultSetBuilder(final CassandraStatement statement) throws SQLException {
        super(statement);
    }

    /**
     * Builds a valid result set of the description of the table columns available in the given catalog (Cassandra
     * cluster).
     * This method is used to implement the method {@link DatabaseMetaData#getColumns(String, String, String, String)}.
     * <p>
     * Only table descriptions matching the catalog, schema, table and column name criteria are returned. They are
     * ordered by {@code TABLE_CAT}, {@code TABLE_SCHEM}, {@code TABLE_NAME} and {@code ORDINAL_POSITION}.
     * </p>
     * <p>
     * The columns of this result set are:
     *     <ol>
     *         <li><b>TABLE_CAT</b> String => table catalog, may be {@code null}: here is the Cassandra cluster name
     *         (if available).</li>
     *         <li><b>TABLE_SCHEM</b> String => table schema, may be {@code null}: here is the keyspace the table is
     *         member of.</li>
     *         <li><b>TABLE_NAME</b> String => table name.</li>
     *         <li><b>COLUMN_NAME</b> String => column name.</li>
     *         <li><b>DATA_TYPE</b> int => SQL type from {@link Types}.</li>
     *         <li><b>TYPE_NAME</b> String => Data source dependent type name, for a UDT the type name is fully
     *         qualified.</li>
     *         <li><b>COLUMN_SIZE</b> int => column size.</li>
     *         <li><b>BUFFER_LENGTH</b> int => not used: always 0 here.</li>
     *         <li><b>DECIMAL_DIGITS</b> int => the number of fractional digits, {@code null} is returned for data
     *         types where it is not applicable. Always {@code null} here.</li>
     *         <li><b>NUM_PREC_RADIX</b> int => Radix (typically either 10 or 2).</li>
     *         <li><b>NULLABLE</b> int => is {@code NULL} allowed:
     *             <ul>
     *                 <li>{@link DatabaseMetaData#columnNoNulls} - might not allow {@code NULL} values</li>
     *                 <li>{@link DatabaseMetaData#columnNullable} - definitely allows {@code NULL} values</li>
     *                 <li>{@link DatabaseMetaData#columnNullableUnknown} - nullability unknown</li>
     *             </ul> Always {@link DatabaseMetaData#columnNoNulls} here.
     *         </li>
     *         <li><b>REMARKS</b> String => comment describing column, may be {@code null}:
     *         always {@code null} here since comments on columns does not exist in Cassandra.</li>
     *         <li><b>COLUMN_DEF</b> String => default value for the column, which should be interpreted as a string
     *         when the value is enclosed in single quotes, may be {@code null}. Always {@code null} here.</li>
     *         <li><b>SQL_DATA_TYPE</b> int => not used: always {@code null} here.</li>
     *         <li><b>SQL_DATETIME_SUB</b> int => is not used: always {@code null} here.</li>
     *         <li><b>CHAR_OCTET_LENGTH</b> int => for char types the maximum number of bytes in the column.</li>
     *         <li><b>ORDINAL_POSITION</b> int => index of column in table (starting at 1).</li>
     *         <li><b>IS_NULLABLE</b> String => ISO rules are used to determine the nullability for a column:
     *             <ul>
     *                 <li><i>YES</i> - if the parameter can include {@code NULL}s</li>
     *                 <li><i>NO</i> - if the parameter cannot include {@code NULL}s</li>
     *                 <li><i>empty string</i> - if the nullability for the parameter is unknown</li>
     *             </ul> Always empty here.
     *         </li>
     *         <li><b>SCOPE_CATALOG</b> String => catalog of table that is the scope of a reference attribute
     *         ({@code null} if {@code DATA_TYPE} isn't REF). Always {@code null} here.</li>
     *         <li><b>SCOPE_SCHEMA</b> String => schema of table that is the scope of a reference attribute
     *         ({@code null} if {@code DATA_TYPE} isn't REF). Always {@code null} here.</li>
     *         <li><b>SCOPE_TABLE</b> String => table name that is the scope of a reference attribute
     *         ({@code null} if {@code DATA_TYPE} isn't REF). Always {@code null} here.</li>
     *         <li><b>SOURCE_DATA_TYPE</b> short => source type of a distinct type or user-generated Ref type, SQL type
     *         from {@link Types} ({@code null} if {@code DATA_TYPE} isn't {@code DISTINCT} or user-generated
     *         {@code REF}). Always {@code null} here.</li>
     *         <li><b>IS_AUTOINCREMENT</b> String => Indicates whether this column is auto-incremented:
     *             <ul>
     *                 <li><i>YES</i> - if the column is auto-incremented</li>
     *                 <li><i>NO</i> - if the column is not auto-incremented</li>
     *                 <li><i>empty string</i> - if it cannot be determined whether the column is auto incremented
     *                 parameter is unknown</li>
     *             </ul> Always {@code NO} here.
     *         </li>
     *         <li><b>IS_GENERATEDCOLUMN</b> String => Indicates whether this is a generated column:
     *             <ul>
     *                 <li><i>YES</i> - if this is a generated column</li>
     *                 <li><i>NO</i> - if this is not a generated column</li>
     *                 <li><i>empty string</i> - if it cannot be determined whether this is a generated column</li>
     *             </ul> Always {@code NO} here.
     *         </li>
     *     </ol>
     *
     * @param schemaPattern     A schema name pattern. It must match the schema name as it is stored in the database;
     *                          {@code ""} retrieves those without a schema and {@code null} means that the schema name
     *                          should not be used to narrow the search. Using {@code ""} as the same effect as
     *                          {@code null} because here the schema corresponds to the keyspace and Cassandra tables
     *                          cannot be defined outside a keyspace.
     * @param tableNamePattern  A table name pattern. It must match the table name as it is stored in the database.
     * @param columnNamePattern A column name pattern. It must match the column name as it is stored in the database.
     * @return A valid result set for implementation of
     * {@link DatabaseMetaData#getColumns(String, String, String, String)}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet buildColumns(final String schemaPattern,
                                                   final String tableNamePattern,
                                                   final String columnNamePattern) throws SQLException {
        final String catalog = this.connection.getCatalog();
        final ArrayList<MetadataRow> columns = new ArrayList<>();


        filterBySchemaNamePattern(schemaPattern, keyspaceMetadata ->
            filterByTableNamePattern(tableNamePattern, keyspaceMetadata, tableMetadata -> {
                final AtomicInteger colIndex = new AtomicInteger(1); // The ordinal positions start at 1.
                filterByColumnNamePattern(columnNamePattern, tableMetadata, columnMetadata -> {
                    final AbstractJdbcType<?> jdbcEquivalentType =
                        getTypeForComparator(columnMetadata.getType().toString());

                    // Define value of COLUMN_SIZE.
                    int columnSize = DEFAULT_PRECISION;
                    if (jdbcEquivalentType != null) {
                        columnSize = jdbcEquivalentType.getPrecision(null);
                    }

                    // Define value of NUM_PREC_RADIX.
                    int radix = 2;
                    if (jdbcEquivalentType != null && (jdbcEquivalentType.getJdbcType() == Types.DECIMAL
                        || jdbcEquivalentType.getJdbcType() == Types.NUMERIC)) {
                        radix = 10;
                    }

                    // Define value of DATA_TYPE.
                    int jdbcType = Types.OTHER;
                    try {
                        jdbcType = getTypeForComparator(columnMetadata.getType().toString())
                            .getJdbcType();
                    } catch (final Exception e) {
                        LOG.warn("Unable to get JDBC type for comparator [{}]: {}",
                            columnMetadata.getType(), e.getMessage());
                    }

                    final MetadataRow row = new MetadataRow()
                        .addEntry(TABLE_CATALOG_SHORTNAME, catalog)
                        .addEntry(TABLE_SCHEMA, keyspaceMetadata.getName().asInternal())
                        .addEntry(TABLE_NAME, tableMetadata.getName().asInternal())
                        .addEntry(COLUMN_NAME, columnMetadata.getName().asInternal())
                        .addEntry(DATA_TYPE, String.valueOf(jdbcType))
                        .addEntry(TYPE_NAME, columnMetadata.getType().toString())
                        .addEntry(COLUMN_SIZE, String.valueOf(columnSize))
                        .addEntry(BUFFER_LENGTH, String.valueOf(0))
                        .addEntry(DECIMAL_DIGITS, null)
                        .addEntry(NUM_PRECISION_RADIX, String.valueOf(radix))
                        .addEntry(NULLABLE, String.valueOf(DatabaseMetaData.columnNoNulls))
                        .addEntry(REMARKS, null)
                        .addEntry(COLUMN_DEFAULT, null)
                        .addEntry(SQL_DATA_TYPE, null)
                        .addEntry(SQL_DATETIME_SUB, null)
                        .addEntry(CHAR_OCTET_LENGTH, String.valueOf(Integer.MAX_VALUE))
                        .addEntry(ORDINAL_POSITION, String.valueOf(colIndex.getAndIncrement()))
                        .addEntry(IS_NULLABLE, StringUtils.EMPTY)
                        .addEntry(SCOPE_CATALOG, null)
                        .addEntry(SCOPE_SCHEMA, null)
                        .addEntry(SCOPE_TABLE, null)
                        .addEntry(SOURCE_DATA_TYPE, null)
                        .addEntry(IS_AUTOINCREMENT, NO_VALUE)
                        .addEntry(IS_GENERATED_COLUMN, NO_VALUE);
                    columns.add(row);
                }, columnMetadata -> colIndex.getAndIncrement());
            }, null), null);

        // Results should all have the same TABLE_CAT, so just sort them by TABLE_SCHEM, TABLE_NAME then
        // ORDINAL_POSITION.
        columns.sort(Comparator.comparing(row -> ((MetadataRow) row).getString(TABLE_SCHEMA))
            .thenComparing(row -> ((MetadataRow) row).getString(TABLE_NAME))
            .thenComparing(row -> ((MetadataRow) row).getString(ORDINAL_POSITION)));
        return CassandraMetadataResultSet.buildFrom(this.statement, new MetadataResultSet().setRows(columns));
    }

}
