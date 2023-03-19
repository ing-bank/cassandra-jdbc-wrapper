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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Map;

import static com.ing.data.cassandra.jdbc.AbstractJdbcType.DEFAULT_PRECISION;

/**
 * Utility class to manage database metadata result sets ({@link CassandraMetadataResultSet} objects).
 */
public final class MetadataResultSets {
    /**
     * Gets an instance of {@code MetadataResultSets}.
     */
    public static final MetadataResultSets INSTANCE = new MetadataResultSets();

    static final String CQL_OPTION_COMMENT = "comment";
    static final String ASC_OR_DESC = "ASC_OR_DESC";
    static final String BUFFER_LENGTH = "BUFFER_LENGTH";
    static final String CARDINALITY = "CARDINALITY";
    static final String CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH";
    static final String COLUMN_DEFAULT = "COLUMN_DEF";
    static final String COLUMN_NAME = "COLUMN_NAME";
    static final String COLUMN_SIZE = "COLUMN_SIZE";
    static final String DATA_TYPE = "DATA_TYPE";
    static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
    static final String FILTER_CONDITION = "FILTER_CONDITION";
    static final String INDEX_NAME = "INDEX_NAME";
    static final String INDEX_QUALIFIER = "INDEX_QUALIFIER";
    static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
    static final String IS_GENERATED_COLUMN = "IS_GENERATEDCOLUMN";
    static final String IS_NULLABLE = "IS_NULLABLE";
    static final String KEY_SEQ = "KEY_SEQ";
    static final String NO_VALUE = "NO";
    static final String NON_UNIQUE = "NON_UNIQUE";
    static final String NULLABLE = "NULLABLE";
    static final String NUM_PREC_RADIX = "NUM_PREC_RADIX";
    static final String ORDINAL_POSITION = "ORDINAL_POSITION";
    static final String PAGES = "PAGES";
    static final String PRIMARY_KEY_NAME = "PK_NAME";
    static final String REF_GENERATION = "REF_GENERATION";
    static final String REMARKS = "REMARKS";
    static final String SCOPE_CATALOG = "SCOPE_CATALOG";
    static final String SCOPE_SCHEMA = "SCOPE_SCHEMA";
    static final String SCOPE_TABLE = "SCOPE_TABLE";
    static final String SELF_REFERENCING_COL_NAME = "SELF_REFERENCING_COL_NAME";
    static final String SOURCE_DATA_TYPE = "SOURCE_DATA_TYPE";
    static final String SQL_DATA_TYPE = "SQL_DATA_TYPE";
    static final String SQL_DATETIME_SUB = "SQL_DATETIME_SUB";
    static final String TABLE = "TABLE";
    static final String TABLE_CATALOG_SHORTNAME = "TABLE_CAT";
    static final String TABLE_CATALOG = "TABLE_CATALOG";
    static final String TABLE_NAME = "TABLE_NAME";
    static final String TABLE_SCHEMA = "TABLE_SCHEM";
    static final String TABLE_TYPE = "TABLE_TYPE";
    static final String TYPE = "TYPE";
    static final String TYPE_CATALOG = "TYPE_CAT";
    static final String TYPE_NAME = "TYPE_NAME";
    static final String TYPE_SCHEMA = "TYPE_SCHEM";
    static final String WILDCARD_CHAR = "%";

    private static final Logger LOG = LoggerFactory.getLogger(MetadataResultSets.class);

    private MetadataResultSets() {
        // Private constructor to hide the public one.
    }

    /**
     * Builds a valid result set of the table types available in Cassandra database. This method is used to implement
     * the method {@link DatabaseMetaData#getTableTypes()}.
     * <p>
     * The columns of this result set are:
     *     <ol>
     *         <li><b>TABLE_TYPE</b> String => table type: always {@value TABLE}.</li>
     *     </ol>
     * </p>
     *
     * @param statement The statement.
     * @return A valid result set for implementation of {@link DatabaseMetaData#getTableTypes()}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet makeTableTypes(final CassandraStatement statement) throws SQLException {
        final ArrayList<MetadataRow> tableTypes = new ArrayList<>();
        final MetadataRow row = new MetadataRow().addEntry(TABLE_TYPE, TABLE);
        tableTypes.add(row);
        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(tableTypes));
    }

    /**
     * Builds a valid result set of the catalog names available in this Cassandra database. This method is used to
     * implement the method {@link DatabaseMetaData#getCatalogs()}.
     * <p>
     * The columns of this result set are:
     *     <ol>
     *         <li><b>TABLE_CAT</b> String => catalog name: here is the Cassandra cluster name (if available).</li>
     *     </ol>
     * </p>
     *
     * @param statement The statement.
     * @return A valid result set for implementation of {@link DatabaseMetaData#getCatalogs()}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet makeCatalogs(final CassandraStatement statement) throws SQLException {
        final ArrayList<MetadataRow> catalog = new ArrayList<>();
        final MetadataRow row = new MetadataRow().addEntry(TABLE_CATALOG_SHORTNAME, statement.connection.getCatalog());
        catalog.add(row);
        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(catalog));
    }

    /**
     * Builds a valid result set of the schema names available in this Cassandra database. This method is used to
     * implement the methods {@link DatabaseMetaData#getSchemas()} and
     * {@link DatabaseMetaData#getSchemas(String, String)}.
     * <p>
     * The columns of this result set are:
     *     <ol>
     *         <li><b>TABLE_SCHEM</b> String => schema name: here is the keyspace name.</li>
     *         <li><b>TABLE_CATALOG</b> String => catalog name, may be {@code null}: here is the Cassandra cluster name
     *         (if available).</li>
     *     </ol>
     * </p>
     *
     * @param statement     The statement.
     * @param schemaPattern A schema name. It must match the schema name as it is stored in the database; {@code null}
     *                      means schema name should not be used to narrow down the search.
     * @return A valid result set for implementation of {@link DatabaseMetaData#getSchemas(String, String)}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet makeSchemas(final CassandraStatement statement, final String schemaPattern)
        throws SQLException {
        final ArrayList<MetadataRow> schemas = new ArrayList<>();
        final Map<CqlIdentifier, KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();

        for (final Map.Entry<CqlIdentifier, KeyspaceMetadata> keyspace : keyspaces.entrySet()) {
            final KeyspaceMetadata keyspaceMetadata = keyspace.getValue();
            String schemaNamePattern = schemaPattern;
            if (WILDCARD_CHAR.equals(schemaPattern)) {
                schemaNamePattern = keyspaceMetadata.getName().asInternal();
            }
            if (schemaNamePattern == null || schemaNamePattern.equals(keyspaceMetadata.getName().asInternal())) {
                final MetadataRow row = new MetadataRow()
                    .addEntry(TABLE_SCHEMA, keyspaceMetadata.getName().asInternal())
                    .addEntry(TABLE_CATALOG, statement.connection.getCatalog());
                schemas.add(row);
            }
        }

        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(schemas));
    }

    /**
     * Builds a valid result set of the description of the tables available in the given catalog (Cassandra cluster).
     * This method is used to implement the method {@link DatabaseMetaData#getTables(String, String, String, String[])}.
     * <p>
     * The columns of this result set are:
     *     <ol>
     *         <li><b>TABLE_CAT</b> String => table catalog, may be {@code null}: here is the Cassandra cluster name
     *         (if available).</li>
     *         <li><b>TABLE_SCHEM</b> String => table schema, may be {@code null}: here is the keyspace the table is
     *         member of.</li>
     *         <li><b>TABLE_NAME</b> String => table name.</li>
     *         <li><b>TABLE_TYPE</b> String => table type: always {@value TABLE} here.</li>
     *         <li><b>REMARKS</b> String => explanatory comment on the table.</li>
     *         <li><b>TYPE_CAT</b> String => the types catalog: always {@code null} here.</li>
     *         <li><b>TYPE_SCHEM</b> String => the types schema: always {@code null} here.</li>
     *         <li><b>TYPE_NAME</b> String => type name: always {@code null} here.</li>
     *         <li><b>SELF_REFERENCING_COL_NAME</b> String => name of the designated "identifier" column of a typed
     *         table: always {@code null} here.</li>
     *         <li><b>REF_GENERATION</b> String =>  specifies how values in {@code SELF_REFERENCING_COL_NAME} are
     *         created: always {@code null} here.</li>
     *     </ol>
     * </p>
     *
     * @param statement        The statement.
     * @param schemaPattern    A schema name pattern. It must match the schema name as it is stored in the database;
     *                         {@code ""} retrieves those without a schema and {@code null} means that the schema name
     *                         should not be used to narrow down the search.
     * @param tableNamePattern A table name pattern. It must match the table name as it is stored in the database.
     * @return A valid result set for implementation of
     * {@link DatabaseMetaData#getTables(String, String, String, String[])}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet makeTables(final CassandraStatement statement, final String schemaPattern,
                                                 final String tableNamePattern) throws SQLException {
        final ArrayList<MetadataRow> schemas = new ArrayList<>();
        final Map<CqlIdentifier, KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();

        for (final Map.Entry<CqlIdentifier, KeyspaceMetadata> keyspace : keyspaces.entrySet()) {
            final KeyspaceMetadata keyspaceMetadata = keyspace.getValue();
            String schemaNamePattern = schemaPattern;
            if (WILDCARD_CHAR.equals(schemaPattern)) {
                schemaNamePattern = keyspaceMetadata.getName().asInternal();
            }
            if (schemaNamePattern == null || schemaNamePattern.equals(keyspaceMetadata.getName().asInternal())) {
                final Map<CqlIdentifier, TableMetadata> tables = keyspaceMetadata.getTables();

                for (final Map.Entry<CqlIdentifier, TableMetadata> table : tables.entrySet()) {
                    final TableMetadata tableMetadata = table.getValue();
                    if (WILDCARD_CHAR.equals(tableNamePattern) || tableNamePattern == null
                        || tableNamePattern.equals(tableMetadata.getName().asInternal())) {
                        final MetadataRow row = new MetadataRow()
                            .addEntry(TABLE_CATALOG_SHORTNAME, statement.connection.getCatalog())
                            .addEntry(TABLE_SCHEMA, keyspaceMetadata.getName().asInternal())
                            .addEntry(TABLE_NAME, tableMetadata.getName().asInternal())
                            .addEntry(TABLE_TYPE, TABLE)
                            .addEntry(REMARKS, tableMetadata.getOptions()
                                .get(CqlIdentifier.fromCql(CQL_OPTION_COMMENT)).toString())
                            .addEntry(TYPE_CATALOG, null)
                            .addEntry(TYPE_SCHEMA, null)
                            .addEntry(TYPE_NAME, null)
                            .addEntry(SELF_REFERENCING_COL_NAME, null)
                            .addEntry(REF_GENERATION, null);
                        schemas.add(row);
                    }
                }
            }
        }

        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(schemas));
    }

    /**
     * Builds a valid result set of the description of the table columns available in the given catalog (Cassandra
     * cluster). This method is used to implement the method
     * {@link DatabaseMetaData#getColumns(String, String, String, String)}.
     *
     * <p>
     * The columns of this result set are:
     *     <ol>
     *         <li><b>TABLE_CAT</b> String => table catalog, may be {@code null}: here is the Cassandra cluster name
     *         (if available).</li>
     *         <li><b>TABLE_SCHEM</b> String => table schema, may be {@code null}: here is the keyspace the table is
     *         member of.</li>
     *         <li><b>TABLE_NAME</b> String => table name.</li>
     *         <li><b>COLUMN_NAME</b> String => column name.</li>
     *         <li><b>DATA_TYPE</b> int => SQL type from {@link java.sql.Types}.</li>
     *         <li><b>TYPE_NAME</b> String => Data source dependent type name, for a UDT the type name is fully
     *         qualified.</li>
     *         <li><b>COLUMN_SIZE</b> int => column size.</li>
     *         <li><b>BUFFER_LENGTH</b> int => not used.</li>
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
     *         <li><b>REMARKS</b> String => comment describing column, may be {@code null}.</li>
     *         <li><b>COLUMN_DEF</b> String => default value for the column, which should be interpreted as a string
     *         when the value is enclosed in single quotes, may be {@code null}. Always {@code null} here.</li>
     *         <li><b>SQL_DATA_TYPE</b> int => not used.</li>
     *         <li><b>SQL_DATETIME_SUB</b> int => is not used.</li>
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
     *         from {@link java.sql.Types} ({@code null} if {@code DATA_TYPE} isn't {@code DISTINCT} or user-generated
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
     * </p>
     *
     * @param statement         The statement.
     * @param schemaPattern     A schema name pattern. It must match the schema name as it is stored in the database;
     *                          {@code ""} retrieves those without a schema and {@code null} means that the schema name
     *                          should not be used to narrow down the search.
     * @param tableNamePattern  A table name pattern. It must match the table name as it is stored in the database.
     * @param columnNamePattern A column name pattern. It must match the column name as it is stored in the database.
     * @return A valid result set for implementation of
     * {@link DatabaseMetaData#getColumns(String, String, String, String)}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet makeColumns(final CassandraStatement statement, final String schemaPattern,
                                                  final String tableNamePattern, final String columnNamePattern)
        throws SQLException {
        String originalSchemaPattern = schemaPattern;
        final ArrayList<MetadataRow> schemas = new ArrayList<>();
        final Map<CqlIdentifier, KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();

        for (final Map.Entry<CqlIdentifier, KeyspaceMetadata> keyspace : keyspaces.entrySet()) {
            final KeyspaceMetadata keyspaceMetadata = keyspace.getValue();
            if (WILDCARD_CHAR.equals(schemaPattern)) {
                originalSchemaPattern = keyspaceMetadata.getName().asInternal();
            }

            if (originalSchemaPattern == null
                || originalSchemaPattern.equals(keyspaceMetadata.getName().asInternal())) {
                final Map<CqlIdentifier, TableMetadata> tables = keyspaceMetadata.getTables();

                for (final Map.Entry<CqlIdentifier, TableMetadata> table : tables.entrySet()) {
                    final TableMetadata tableMetadata = table.getValue();
                    if (WILDCARD_CHAR.equals(tableNamePattern) || tableNamePattern == null
                        || tableNamePattern.equals(tableMetadata.getName().asInternal())) {
                        final Map<CqlIdentifier, ColumnMetadata> columns = tableMetadata.getColumns();

                        int columnIndex = 1;
                        for (final Map.Entry<CqlIdentifier, ColumnMetadata> column : columns.entrySet()) {
                            final ColumnMetadata columnMetadata = column.getValue();
                            if (WILDCARD_CHAR.equals(columnNamePattern) || columnNamePattern == null
                                || columnNamePattern.equals(columnMetadata.getName().asInternal())) {
                                final AbstractJdbcType<?> jdbcEquivalentType =
                                    TypesMap.getTypeForComparator(columnMetadata.getType().toString());

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
                                    jdbcType = TypesMap.getTypeForComparator(columnMetadata.getType().toString())
                                        .getJdbcType();
                                } catch (final Exception e) {
                                    LOG.warn("Unable to get JDBC type for comparator [{}]: {}",
                                        columnMetadata.getType(), e.getMessage());
                                }

                                // Build the metadata row.
                                final MetadataRow row = new MetadataRow()
                                    .addEntry(TABLE_CATALOG_SHORTNAME, statement.connection.getCatalog())
                                    .addEntry(TABLE_SCHEMA, keyspaceMetadata.getName().asInternal())
                                    .addEntry(TABLE_NAME, tableMetadata.getName().asInternal())
                                    .addEntry(COLUMN_NAME, columnMetadata.getName().asInternal())
                                    .addEntry(DATA_TYPE, String.valueOf(jdbcType))
                                    .addEntry(TYPE_NAME, columnMetadata.getType().toString())
                                    .addEntry(COLUMN_SIZE, String.valueOf(columnSize))
                                    .addEntry(BUFFER_LENGTH, String.valueOf(0))
                                    .addEntry(DECIMAL_DIGITS, null)
                                    .addEntry(NUM_PREC_RADIX, String.valueOf(radix))
                                    .addEntry(NULLABLE, String.valueOf(DatabaseMetaData.columnNoNulls))
                                    .addEntry(REMARKS, column.toString())
                                    .addEntry(COLUMN_DEFAULT, null)
                                    .addEntry(SQL_DATA_TYPE, null)
                                    .addEntry(SQL_DATETIME_SUB, null)
                                    .addEntry(CHAR_OCTET_LENGTH, String.valueOf(Integer.MAX_VALUE))
                                    .addEntry(ORDINAL_POSITION, String.valueOf(columnIndex))
                                    .addEntry(IS_NULLABLE, StringUtils.EMPTY)
                                    .addEntry(SCOPE_CATALOG, null)
                                    .addEntry(SCOPE_SCHEMA, null)
                                    .addEntry(SCOPE_TABLE, null)
                                    .addEntry(SOURCE_DATA_TYPE, null)
                                    .addEntry(IS_AUTOINCREMENT, NO_VALUE)
                                    .addEntry(IS_GENERATED_COLUMN, NO_VALUE);
                                schemas.add(row);
                                columnIndex++;
                            }
                        }
                    }
                }
            }
        }

        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(schemas));
    }


    /**
     * Builds a valid result set of the description of given table's indices and statistics.
     * This method is used to implement the method
     * {@link DatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)}.
     * <p>
     * The columns of this result set are:
     *     <ol>
     *         <li><b>TABLE_CAT</b> String => table catalog, may be {@code null}: here is the Cassandra cluster name
     *         (if available).</li>
     *         <li><b>TABLE_SCHEM</b> String => table schema, may be {@code null}: here is the keyspace the table is
     *         member of.</li>
     *         <li><b>TABLE_NAME</b> String => table name.</li>
     *         <li><b>NON_UNIQUE</b> boolean => Can index values be non-unique, {@code false} when {@code TYPE} is
     *         {@link DatabaseMetaData#tableIndexStatistic}. Always {@code true} here.</li>
     *         <li><b>INDEX_QUALIFIER</b> String => index catalog, {@code null} when {@code TYPE} is
     *         {@link DatabaseMetaData#tableIndexStatistic}.</li>
     *         <li><b>INDEX_NAME</b> String => index name, {@code null} when {@code TYPE} is
     *         {@link DatabaseMetaData#tableIndexStatistic}.</li>
     *         <li><b>TYPE</b> short => index type:
     *             <ul>
     *                 <li>{@link DatabaseMetaData#tableIndexStatistic} - this identifies table statistics that are
     *                 returned in conjunction with a table's index descriptions</li>
     *                 <li>{@link DatabaseMetaData#tableIndexClustered} - this is a clustered index</li>
     *                 <li>{@link DatabaseMetaData#tableIndexHashed} - this is a hashed index</li>
     *                 <li>{@link DatabaseMetaData#tableIndexOther} - this is some other style of index</li>
     *             </ul> Always {@link DatabaseMetaData#tableIndexHashed} here.
     *         </li>
     *         <li><b>ORDINAL_POSITION</b> short => column sequence number within index; zero when {@code TYPE} is
     *         {@link DatabaseMetaData#tableIndexStatistic}. Always 1 here.</li>
     *         <li><b>COLUMN_NAME</b> String => column name, {@code null} when {@code TYPE} is
     *         {@link DatabaseMetaData#tableIndexStatistic}.</li>
     *         <li><b>ASC_OR_DESC</b> String => column sort sequence, "A" means ascending, "D" means descending, may be
     *         {@code null} if sort sequence is not supported or when {@code TYPE} is
     *         {@link DatabaseMetaData#tableIndexStatistic}. Always {@code null} here.</li>
     *         <li><b>CARDINALITY</b> int => When {@code TYPE} is {@link DatabaseMetaData#tableIndexStatistic}, then
     *         this is the number of rows in the table; otherwise, it is the number of unique values in the index.
     *         Always -1 here.</li>
     *         <li><b>PAGES</b> int => When {@code TYPE} is {@link DatabaseMetaData#tableIndexStatistic}, then
     *         this is the number of pages used for the table; otherwise, it is the number of pages used for the
     *         current index. Always -1 here.</li>
     *         <li><b>FILTER_CONDITION</b> String => Filter condition, if any: always {@code null} here.</li>
     *     </ol>
     * </p>
     *
     * @param statement   The statement.
     * @param schema      A schema name. It must match the schema name as it is stored in the database; {@code ""}
     *                    retrieves those without a schema and {@code null} means that the schema name should not be
     *                    used to narrow down the search.
     * @param tableName   A table name. It must match the table name as it is stored in the database.
     * @param unique      when {@code true}, return only indices for unique values; when {@code false}, return
     *                    indices regardless of whether unique or not.
     * @param approximate when {@code true}, result is allowed to reflect approximate or out of data values; when
     *                    {@code false}, results are requested to be accurate.
     * @return A valid result set for implementation of
     * {@link DatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    @SuppressWarnings("unused")
    public CassandraMetadataResultSet makeIndexes(final CassandraStatement statement, final String schema,
                                                  final String tableName, final boolean unique,
                                                  final boolean approximate) throws SQLException {
        final ArrayList<MetadataRow> schemas = new ArrayList<>();
        final Map<CqlIdentifier, KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();

        for (final Map.Entry<CqlIdentifier, KeyspaceMetadata> keyspace : keyspaces.entrySet()) {
            final KeyspaceMetadata keyspaceMetadata = keyspace.getValue();
            if (schema.equals(keyspaceMetadata.getName().asInternal())) {
                final Map<CqlIdentifier, TableMetadata> tables = keyspaceMetadata.getTables();

                for (final Map.Entry<CqlIdentifier, TableMetadata> table : tables.entrySet()) {
                    final TableMetadata tableMetadata = table.getValue();
                    if (tableName.equals(tableMetadata.getName().asInternal())) {
                        for (final Map.Entry<CqlIdentifier, IndexMetadata> index
                            : tableMetadata.getIndexes().entrySet()) {
                            final IndexMetadata indexMetadata = index.getValue();
                            final MetadataRow row = new MetadataRow()
                                .addEntry(TABLE_CATALOG_SHORTNAME, statement.connection.getCatalog())
                                .addEntry(TABLE_SCHEMA, keyspaceMetadata.getName().asInternal())
                                .addEntry(TABLE_NAME, tableMetadata.getName().asInternal())
                                .addEntry(NON_UNIQUE, Boolean.TRUE.toString())
                                .addEntry(INDEX_QUALIFIER, statement.connection.getCatalog())
                                .addEntry(INDEX_NAME, indexMetadata.getName().asInternal())
                                .addEntry(TYPE, String.valueOf(DatabaseMetaData.tableIndexHashed))
                                .addEntry(ORDINAL_POSITION, String.valueOf(1))
                                .addEntry(COLUMN_NAME, indexMetadata.getTarget())
                                .addEntry(ASC_OR_DESC, null)
                                .addEntry(CARDINALITY, String.valueOf(-1))
                                .addEntry(PAGES, String.valueOf(-1))
                                .addEntry(FILTER_CONDITION, null);
                            schemas.add(row);
                        }
                    }
                }
            }
        }

        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(schemas));
    }

    /**
     * Builds a valid result set of the description of given table's primary key columns.
     * This method is used to implement the method {@link DatabaseMetaData#getPrimaryKeys(String, String, String)}.
     * <p>
     * The columns of this result set are:
     *     <ol>
     *         <li><b>TABLE_CAT</b> String => table catalog, may be {@code null}: here is the Cassandra cluster name
     *         (if available).</li>
     *         <li><b>TABLE_SCHEM</b> String => table schema, may be {@code null}: here is the keyspace the table is
     *         member of.</li>
     *         <li><b>TABLE_NAME</b> String => table name.</li>
     *         <li><b>COLUMN_NAME</b> String => column name.</li>
     *         <li><b>KEY_SEQ</b> short => sequence number within primary key (a value of 1 represents the first column
     *         of the primary key, a value of 2 would represent the second column within the primary key).</li>
     *         <li><b>PK_NAME</b> String => primary key name: always {@code null} here.</li>
     *     </ol>
     * </p>
     *
     * @param statement The statement.
     * @param schema    A schema name. It must match the schema name as it is stored in the database; {@code ""}
     *                  retrieves those without a schema and {@code null} means that the schema name should not be
     *                  used to narrow down the search.
     * @param tableName A table name. It must match the table name as it is stored in the database.
     * @return A valid result set for implementation of {@link DatabaseMetaData#getPrimaryKeys(String, String, String)}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet makePrimaryKeys(final CassandraStatement statement, final String schema,
                                                      final String tableName) throws SQLException {
        final ArrayList<MetadataRow> schemas = new ArrayList<>();
        final Map<CqlIdentifier, KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();

        for (final Map.Entry<CqlIdentifier, KeyspaceMetadata> keyspace : keyspaces.entrySet()) {
            final KeyspaceMetadata keyspaceMetadata = keyspace.getValue();
            if (schema.equals(keyspaceMetadata.getName().asInternal())) {
                final Map<CqlIdentifier, TableMetadata> tables = keyspaceMetadata.getTables();

                for (final Map.Entry<CqlIdentifier, TableMetadata> table : tables.entrySet()) {
                    final TableMetadata tableMetadata = table.getValue();
                    if (tableName.equals(tableMetadata.getName().asInternal())) {
                        int seq = 0;
                        for (final ColumnMetadata col : tableMetadata.getPrimaryKey()) {
                            final MetadataRow row = new MetadataRow()
                                .addEntry(TABLE_CATALOG_SHORTNAME, statement.connection.getCatalog())
                                .addEntry(TABLE_SCHEMA, keyspaceMetadata.getName().asInternal())
                                .addEntry(TABLE_NAME, tableMetadata.getName().asInternal())
                                .addEntry(COLUMN_NAME, col.getName().asInternal())
                                .addEntry(KEY_SEQ, String.valueOf(seq))
                                .addEntry(PRIMARY_KEY_NAME, null);
                            schemas.add(row);
                            seq++;
                        }
                    }
                }
            }
        }

        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(schemas));
    }

}
