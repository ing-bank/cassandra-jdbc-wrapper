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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;

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

}
