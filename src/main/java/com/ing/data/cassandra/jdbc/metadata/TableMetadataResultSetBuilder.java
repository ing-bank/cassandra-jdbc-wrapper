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
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.ing.data.cassandra.jdbc.CassandraMetadataResultSet;
import com.ing.data.cassandra.jdbc.CassandraStatement;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

/**
 * Utility class building metadata result sets ({@link CassandraMetadataResultSet} objects) related to tables.
 */
public class TableMetadataResultSetBuilder extends AbstractMetadataResultSetBuilder {

    /**
     * Constructor.
     *
     * @param statement The statement.
     * @throws SQLException if a database access error occurs or this statement is closed.
     */
    public TableMetadataResultSetBuilder(final CassandraStatement statement) throws SQLException {
        super(statement);
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
     * @return A valid result set for implementation of {@link DatabaseMetaData#getTableTypes()}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet buildTableTypes() throws SQLException {
        final ArrayList<MetadataRow> tableTypes = new ArrayList<>();
        final MetadataRow row = new MetadataRow().addEntry(TABLE_TYPE, TABLE);
        tableTypes.add(row);
        return CassandraMetadataResultSet.buildFrom(this.statement, new MetadataResultSet().setRows(tableTypes));
    }

    /**
     * Builds a valid result set of the description of the tables available in the given catalog (Cassandra cluster).
     * This method is used to implement the method {@link DatabaseMetaData#getTables(String, String, String, String[])}.
     * <p>
     * Only table descriptions matching the catalog, schema, table name and type criteria are returned. They are
     * ordered by {@code TABLE_CAT}, {@code TABLE_SCHEM} and {@code TABLE_NAME}.
     * </p>
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
     * @param schemaPattern    A schema name pattern. It must match the schema name as it is stored in the database;
     *                         {@code ""} retrieves those without a schema and {@code null} means that the schema name
     *                         should not be used to narrow the search. Using {@code ""} as the same effect as
     *                         {@code null} because here the schema corresponds to the keyspace and Cassandra tables
     *                         cannot be defined outside a keyspace.
     * @param tableNamePattern A table name pattern. It must match the table name as it is stored in the database.
     * @return A valid result set for implementation of
     * {@link DatabaseMetaData#getTables(String, String, String, String[])}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet buildTables(final String schemaPattern,
                                                  final String tableNamePattern) throws SQLException {
        final String catalog = this.connection.getCatalog();
        final ArrayList<MetadataRow> tables = new ArrayList<>();

        filterBySchemaNamePattern(schemaPattern, keyspaceMetadata ->
            filterByTableNamePattern(tableNamePattern, keyspaceMetadata, tableMetadata -> {
                final MetadataRow row = new MetadataRow()
                    .addEntry(TABLE_CATALOG_SHORTNAME, catalog)
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
                tables.add(row);
            }, null), null);

        // Results should all have the same TABLE_CAT, so just sort them by TABLE_SCHEM then TABLE_NAME.
        tables.sort(Comparator.comparing(row -> ((MetadataRow) row).getString(TABLE_SCHEMA))
            .thenComparing(row -> ((MetadataRow) row).getString(TABLE_NAME)));
        return CassandraMetadataResultSet.buildFrom(this.statement, new MetadataResultSet().setRows(tables));
    }

    /**
     * Builds a valid result set of the description of the given table's indices and statistics.
     * This method is used to implement the method
     * {@link DatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)}.
     * <p>
     *     Only indexes of the table exactly matching the catalog, schema and table name are returned. They are
     *     ordered by {@code NON_UNIQUE}, {@code TYPE}, {@code INDEX_NAME} and {@code ORDINAL_POSITION}.
     * </p>
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
     * @param schema      A schema name. It must match the schema name as it is stored in the database; {@code ""}
     *                    retrieves those without a schema and {@code null} means that the schema name should not be
     *                    used to narrow down the search.
     * @param tableName   A table name. It must match the table name as it is stored in the database.
     * @param unique      when {@code true}, return only indices for unique values; when {@code false}, return
     *                    indices regardless of whether unique or not. This parameter has no effect here.
     * @param approximate when {@code true}, result is allowed to reflect approximate or out of data values; when
     *                    {@code false}, results are requested to be accurate. This parameter has no effect here.
     * @return A valid result set for implementation of
     * {@link DatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    @SuppressWarnings("unused")
    public CassandraMetadataResultSet buildIndexes(final String schema,
                                                   final String tableName, final boolean unique,
                                                   final boolean approximate) throws SQLException {
        final String catalog = this.connection.getCatalog();
        final ArrayList<MetadataRow> indexes = new ArrayList<>();

        filterBySchemaNamePattern(schema, keyspaceMetadata ->
            filterByTableNamePattern(tableName, keyspaceMetadata, tableMetadata -> {
                for (final Map.Entry<CqlIdentifier, IndexMetadata> index : tableMetadata.getIndexes().entrySet()) {
                    final IndexMetadata indexMetadata = index.getValue();
                    final MetadataRow row = new MetadataRow()
                        .addEntry(TABLE_CATALOG_SHORTNAME, catalog)
                        .addEntry(TABLE_SCHEMA, keyspaceMetadata.getName().asInternal())
                        .addEntry(TABLE_NAME, tableMetadata.getName().asInternal())
                        .addEntry(NON_UNIQUE, Boolean.TRUE.toString())
                        .addEntry(INDEX_QUALIFIER, catalog)
                        .addEntry(INDEX_NAME, indexMetadata.getName().asInternal())
                        .addEntry(TYPE, String.valueOf(DatabaseMetaData.tableIndexHashed))
                        .addEntry(ORDINAL_POSITION, String.valueOf(1))
                        .addEntry(COLUMN_NAME, indexMetadata.getTarget())
                        .addEntry(ASC_OR_DESC, null)
                        .addEntry(CARDINALITY, String.valueOf(-1))
                        .addEntry(PAGES, String.valueOf(-1))
                        .addEntry(FILTER_CONDITION, null);
                    indexes.add(row);
                }
            }, null), null);

        // Results should all have the same NON_UNIQUE, TYPE and ORDINAL_POSITION, so just sort them by INDEX_NAME.
        indexes.sort(Comparator.comparing(row -> row.getString(INDEX_NAME)));
        return CassandraMetadataResultSet.buildFrom(this.statement, new MetadataResultSet().setRows(indexes));
    }

    /**
     * Builds a valid result set of the description of the given table's primary key columns.
     * This method is used to implement the method {@link DatabaseMetaData#getPrimaryKeys(String, String, String)}.
     * <p>
     *     Only primary keys of the table exactly matching the catalog, schema and table name are returned. They are
     *     ordered by {@code COLUMN_NAME}.
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
     *         <li><b>KEY_SEQ</b> short => sequence number within primary key (a value of 1 represents the first column
     *         of the primary key, a value of 2 would represent the second column within the primary key).</li>
     *         <li><b>PK_NAME</b> String => primary key name: always {@code null} here.</li>
     *     </ol>
     * </p>
     *
     * @param schema    A schema name. It must match the schema name as it is stored in the database; {@code ""}
     *                  retrieves those without a schema and {@code null} means that the schema name should not be
     *                  used to narrow down the search.
     * @param tableName A table name. It must match the table name as it is stored in the database.
     * @return A valid result set for implementation of {@link DatabaseMetaData#getPrimaryKeys(String, String, String)}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet buildPrimaryKeys(final String schema, final String tableName)
        throws SQLException {
        final String catalog = this.connection.getCatalog();
        final ArrayList<MetadataRow> primaryKeys = new ArrayList<>();

        filterBySchemaNamePattern(schema, keyspaceMetadata ->
            filterByTableNamePattern(tableName, keyspaceMetadata, tableMetadata -> {
                int seq = 1;
                for (final ColumnMetadata col : tableMetadata.getPrimaryKey()) {
                    final MetadataRow row = new MetadataRow()
                        .addEntry(TABLE_CATALOG_SHORTNAME, catalog)
                        .addEntry(TABLE_SCHEMA, keyspaceMetadata.getName().asInternal())
                        .addEntry(TABLE_NAME, tableMetadata.getName().asInternal())
                        .addEntry(COLUMN_NAME, col.getName().asInternal())
                        .addEntry(KEY_SEQ, String.valueOf(seq))
                        .addEntry(PRIMARY_KEY_NAME, null);
                    primaryKeys.add(row);
                    seq++;
                }
            }, null), null);

        // Sort the results by COLUMN_NAME.
        primaryKeys.sort(Comparator.comparing(row -> row.getString(COLUMN_NAME)));
        return CassandraMetadataResultSet.buildFrom(statement, new MetadataResultSet().setRows(primaryKeys));
    }

}
