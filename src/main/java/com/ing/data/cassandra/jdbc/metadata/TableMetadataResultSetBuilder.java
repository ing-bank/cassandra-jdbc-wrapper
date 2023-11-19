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
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.ing.data.cassandra.jdbc.CassandraMetadataResultSet;
import com.ing.data.cassandra.jdbc.CassandraStatement;
import com.ing.data.cassandra.jdbc.ColumnDefinitions;
import com.ing.data.cassandra.jdbc.types.AbstractJdbcType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

import static com.ing.data.cassandra.jdbc.ColumnDefinitions.Definition.buildDefinitionInAnonymousTable;
import static com.ing.data.cassandra.jdbc.types.AbstractJdbcType.DEFAULT_PRECISION;
import static com.ing.data.cassandra.jdbc.types.TypesMap.getTypeForComparator;
import static java.sql.DatabaseMetaData.bestRowNotPseudo;

/**
 * Utility class building metadata result sets ({@link CassandraMetadataResultSet} objects) related to tables.
 */
public class TableMetadataResultSetBuilder extends AbstractMetadataResultSetBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(TableMetadataResultSetBuilder.class);

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
        final MetadataRow.MetadataRowTemplate rowTemplate = new MetadataRow.MetadataRowTemplate(
            buildDefinitionInAnonymousTable(TABLE_TYPE, DataTypes.TEXT)
        );

        tableTypes.add(new MetadataRow().withTemplate(rowTemplate, TABLE));

        return CassandraMetadataResultSet.buildFrom(this.statement,
            new MetadataResultSet(rowTemplate).setRows(tableTypes));
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
        final MetadataRow.MetadataRowTemplate rowTemplate = new MetadataRow.MetadataRowTemplate(
            buildDefinitionInAnonymousTable(TABLE_CATALOG_SHORTNAME, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(TABLE_SCHEMA, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(TABLE_NAME, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(TABLE_TYPE, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(REMARKS, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(TYPE_CATALOG, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(TYPE_SCHEMA, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(TYPE_NAME, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(SELF_REFERENCING_COL_NAME, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(REF_GENERATION, DataTypes.TEXT)
        );

        filterBySchemaNamePattern(schemaPattern, keyspaceMetadata ->
            filterByTableNamePattern(tableNamePattern, keyspaceMetadata, tableMetadata -> {
                final MetadataRow row = new MetadataRow().withTemplate(rowTemplate,
                    catalog,                                   // TABLE_CAT
                    keyspaceMetadata.getName().asInternal(),   // TABLE_SCHEM
                    tableMetadata.getName().asInternal(),      // TABLE_NAME
                    TABLE,                                     // TABLE_TYPE
                    tableMetadata.getOptions().get(CqlIdentifier.fromCql(CQL_OPTION_COMMENT)).toString(), // REMARKS
                    null,                                      // TYPE_CAT
                    null,                                      // TYPE_SCHEM
                    null,                                      // TYPE_NAME
                    null,                                      // SELF_REFERENCING_COL_NAME
                    null);                                     // REF_GENERATION
                tables.add(row);
            }, null), null);

        // Results should all have the same TABLE_CAT, so just sort them by TABLE_SCHEM then TABLE_NAME.
        tables.sort(Comparator.comparing(row -> ((MetadataRow) row).getString(TABLE_SCHEMA))
            .thenComparing(row -> ((MetadataRow) row).getString(TABLE_NAME)));
        return CassandraMetadataResultSet.buildFrom(this.statement,
            new MetadataResultSet(rowTemplate).setRows(tables));
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
        final MetadataRow.MetadataRowTemplate rowTemplate = new MetadataRow.MetadataRowTemplate(
            buildDefinitionInAnonymousTable(TABLE_CATALOG_SHORTNAME, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(TABLE_SCHEMA, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(TABLE_NAME, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(NON_UNIQUE, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(INDEX_QUALIFIER, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(INDEX_NAME, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(TYPE, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(ORDINAL_POSITION, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(COLUMN_NAME, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(ASC_OR_DESC, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(CARDINALITY, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(PAGES, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(FILTER_CONDITION, DataTypes.TEXT)
        );

        filterBySchemaNamePattern(schema, keyspaceMetadata ->
            filterByTableNamePattern(tableName, keyspaceMetadata, tableMetadata -> {
                for (final Map.Entry<CqlIdentifier, IndexMetadata> index : tableMetadata.getIndexes().entrySet()) {
                    final IndexMetadata indexMetadata = index.getValue();
                    final MetadataRow row = new MetadataRow().withTemplate(rowTemplate,
                        catalog,                                          // TABLE_CAT
                        keyspaceMetadata.getName().asInternal(),          // TABLE_SCHEM
                        tableMetadata.getName().asInternal(),             // TABLE_NAME
                        Boolean.TRUE.toString(),                          // NON_UNIQUE
                        catalog,                                          // INDEX_QUALIFIER
                        indexMetadata.getName().asInternal(),             // INDEX_NAME
                        String.valueOf(DatabaseMetaData.tableIndexOther), // TYPE
                        String.valueOf(1),                                // ORDINAL_POSITION
                        indexMetadata.getTarget(),                        // COLUMN_NAME
                        null,                                             // ASC_OR_DESC
                        String.valueOf(-1),                               // CARDINALITY
                        String.valueOf(-1),                               // PAGES
                        null);                                            // FILTER_CONDITION
                    indexes.add(row);
                }
            }, null), null);

        // Results should all have the same NON_UNIQUE, TYPE and ORDINAL_POSITION, so just sort them by INDEX_NAME.
        indexes.sort(Comparator.comparing(row -> row.getString(INDEX_NAME)));
        return CassandraMetadataResultSet.buildFrom(this.statement,
            new MetadataResultSet(rowTemplate).setRows(indexes));
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
        final MetadataRow.MetadataRowTemplate rowTemplate = new MetadataRow.MetadataRowTemplate(
            buildDefinitionInAnonymousTable(TABLE_CATALOG_SHORTNAME, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(TABLE_SCHEMA, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(TABLE_NAME, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(COLUMN_NAME, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(KEY_SEQ, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(PRIMARY_KEY_NAME, DataTypes.TEXT)
        );

        filterBySchemaNamePattern(schema, keyspaceMetadata ->
            filterByTableNamePattern(tableName, keyspaceMetadata, tableMetadata -> {
                int seq = 1;
                for (final ColumnMetadata col : tableMetadata.getPrimaryKey()) {
                    final MetadataRow row = new MetadataRow().withTemplate(rowTemplate,
                        catalog,                                  // TABLE_CAT
                        keyspaceMetadata.getName().asInternal(),  // TABLE_SCHEM
                        tableMetadata.getName().asInternal(),     // TABLE_NAME
                        col.getName().asInternal(),               // COLUMN_NAME
                        String.valueOf(seq),                      // KEY_SEQ
                        null);                                    // PK_NAME
                    primaryKeys.add(row);
                    seq++;
                }
            }, null), null);

        // Sort the results by COLUMN_NAME.
        primaryKeys.sort(Comparator.comparing(row -> row.getString(COLUMN_NAME)));
        return CassandraMetadataResultSet.buildFrom(this.statement,
            new MetadataResultSet(rowTemplate).setRows(primaryKeys));
    }

    /**
     * Builds a valid result set of the description of a table's optimal set of columns that uniquely identifies a row.
     * This method is used to implement the method
     * {@link DatabaseMetaData#getBestRowIdentifier(String, String, String, int, boolean)}.
     * <p>
     *     In Cassandra, all the tables must define a single primary key and the columns defining this primary key
     *     ensure the uniqueness of each row. So, we consider in this implementation that the best row identifier for
     *     a table is always its primary key regardless of the specified scope. Also, the parameter {@code nullable} has
     *     no effect here since Cassandra does not allow null values in primary keys.
     * </p>
     * <p>
     *     Only identifiers for tables matching the catalog, schema and table name criteria are returned. They are
     *     ordered by {@code SCOPE}.
     * </p>
     * <p>
     * The columns of this result set are:
     *     <ol>
     *         <li><b>SCOPE</b> short => actual scope of result:
     *             <ul>
     *                 <li>{@link DatabaseMetaData#bestRowTemporary} - very temporary, while using row</li>
     *                 <li>{@link DatabaseMetaData#bestRowTransaction} - valid for remainder of current transaction</li>
     *                 <li>{@link DatabaseMetaData#bestRowSession} - valid for remainder of current session</li>
     *             </ul> Always the input scope value.
     *         </li>
     *         <li><b>COLUMN_NAME</b> String => column name.</li>
     *         <li><b>DATA_TYPE</b> int => SQL type from {@link Types}.</li>
     *         <li><b>TYPE_NAME</b> String => Data source dependent type name, for a UDT the type name is fully
     *         qualified.</li>
     *         <li><b>COLUMN_SIZE</b> int => column size.</li>
     *         <li><b>BUFFER_LENGTH</b> int => not used: always 0 here.</li>
     *         <li><b>DECIMAL_DIGITS</b> int => the number of fractional digits, {@code null} is returned for data
     *         types where it is not applicable. Always {@code null} here.</li>
     *         <li><b>PSEUDO_COLUMN</b> short => is this a pseudo column like an Oracle ROWID:
     *             <ul>
     *                 <li>{@link DatabaseMetaData#bestRowUnknown} - may or may not be pseudo column</li>
     *                 <li>{@link DatabaseMetaData#bestRowNotPseudo} - is not a pseudo column</li>
     *                 <li>{@link DatabaseMetaData#bestRowPseudo} - is a pseudo column</li>
     *             </ul> Always {@link DatabaseMetaData#bestRowNotPseudo} here since there is no concept of pseudo
     *             column in Cassandra.
     *         </li>
     *     </ol>
     * </p>
     *
     * @param schema   A schema name pattern. It must match the schema name as it is stored in the database; {@code ""}
     *                 retrieves those without a schema and {@code null} means that the schema name should not be used
     *                 to narrow the search. Using {@code ""} as the same effect as {@code null} because here the schema
     *                 corresponds to the keyspace and Cassandra tables cannot be defined outside a keyspace.
     * @param table    A table name. It must match the table name as it is stored in the database.
     * @param scope    The scope of interest, using the same values as {@code SCOPE} in the result set.
     * @return A valid result set for implementation of
     * {@link DatabaseMetaData#getBestRowIdentifier(String, String, String, int, boolean)}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet buildBestRowIdentifier(final String schema, final String table, final int scope)
        throws SQLException {
        final ArrayList<MetadataRow> bestRowIdentifiers = new ArrayList<>();
        final MetadataRow.MetadataRowTemplate rowTemplate = new MetadataRow.MetadataRowTemplate(
            buildDefinitionInAnonymousTable(SCOPE, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(COLUMN_NAME, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(DATA_TYPE, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(TYPE_NAME, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(COLUMN_SIZE, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(BUFFER_LENGTH, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(DECIMAL_DIGITS, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(PSEUDO_COLUMN, DataTypes.TEXT)
        );

        filterBySchemaNamePattern(schema, keyspaceMetadata ->
            filterByTableNamePattern(table, keyspaceMetadata, tableMetadata -> {
                for (final ColumnMetadata columnMetadata : tableMetadata.getPrimaryKey()) {

                    final AbstractJdbcType<?> jdbcEquivalentType =
                        getTypeForComparator(columnMetadata.getType().toString());

                    // Define value of COLUMN_SIZE.
                    int columnSize = DEFAULT_PRECISION;
                    if (jdbcEquivalentType != null) {
                        columnSize = jdbcEquivalentType.getPrecision(null);
                    }

                    // Define value of DATA_TYPE.
                    int jdbcType = Types.OTHER;
                    try {
                        jdbcType = getTypeForComparator(columnMetadata.getType().toString()).getJdbcType();
                    } catch (final Exception e) {
                        LOG.warn("Unable to get JDBC type for comparator [{}]: {}",
                            columnMetadata.getType(), e.getMessage());
                    }

                    final MetadataRow row = new MetadataRow().withTemplate(rowTemplate,
                            String.valueOf(scope),                 // SCOPE
                            columnMetadata.getName().asInternal(), // COLUMN_NAME
                            String.valueOf(jdbcType),              // DATA_TYPE
                            columnMetadata.getType().toString(),   // TYPE_NAME
                            String.valueOf(columnSize),            // COLUMN_SIZE
                            String.valueOf(0),                     // BUFFER_LENGTH
                            null,                                  // DECIMAL_DIGITS
                            String.valueOf(bestRowNotPseudo));     // PSEUDO_COLUMN
                    bestRowIdentifiers.add(row);
                }
            }, null), null);

        // All the rows of the result set have the same scope, so there is no need to perform an additional sort.
        return CassandraMetadataResultSet.buildFrom(this.statement,
            new MetadataResultSet(rowTemplate).setRows(bestRowIdentifiers));
    }
}
