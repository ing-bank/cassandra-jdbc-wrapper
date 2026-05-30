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

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.ing.data.cassandra.jdbc.CassandraMetadataResultSet;
import com.ing.data.cassandra.jdbc.CassandraStatement;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.ing.data.cassandra.jdbc.CassandraMetadataResultSet.buildFrom;
import static com.ing.data.cassandra.jdbc.ColumnDefinitions.Definition.buildDefinitionInAnonymousTable;
import static java.util.Comparator.comparing;

/**
 * Utility class building metadata result sets ({@link CassandraMetadataResultSet} objects) related to schemas.
 */
public class SchemaMetadataResultSetBuilder extends AbstractMetadataResultSetBuilder {

    /**
     * Constructor.
     *
     * @param statement The statement.
     * @throws SQLException if a database access error occurs or this statement is closed.
     */
    public SchemaMetadataResultSetBuilder(final CassandraStatement statement) throws SQLException {
        super(statement);
    }

    /**
     * Builds a valid result set of the schema names available in this Cassandra database. This method is used to
     * implement the methods {@link DatabaseMetaData#getSchemas()} and
     * {@link DatabaseMetaData#getSchemas(String, String)}. The results are ordered by {@code TABLE_CATALOG}, then
     * {@code TABLE_SCHEMA}.
     * <p>
     * The columns of this result set are:
     *     <ol>
     *         <li><b>TABLE_SCHEM</b> String => schema name: here is the keyspace name.</li>
     *         <li><b>TABLE_CATALOG</b> String => catalog name, may be {@code null}: here is the Cassandra cluster name
     *         (if available).</li>
     *     </ol>
     * </p>
     *
     * @param schemaPattern    A schema name pattern. It must match the schema name as it is stored in the database;
     *                         {@code null} means that the schema name should not be used to narrow the search. Using
     *                         {@code ""} as the same effect as {@code null} because here the schema corresponds to the
     *                         keyspace and Cassandra tables cannot be defined outside a keyspace.
     * @return A valid result set for implementation of {@link DatabaseMetaData#getSchemas(String, String)}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet buildSchemas(final String schemaPattern)
        throws SQLException {
        final ArrayList<MetadataRow> schemas = new ArrayList<>();
        final String catalog = this.connection.getCatalog();
        final MetadataRow.MetadataRowTemplate rowTemplate = new MetadataRow.MetadataRowTemplate(
            buildDefinitionInAnonymousTable(TABLE_SCHEMA, DataTypes.TEXT),
            buildDefinitionInAnonymousTable(TABLE_CATALOG, DataTypes.TEXT)
        );

        filterBySchemaNamePattern(schemaPattern, keyspaceMetadata -> {
            final MetadataRow row = new MetadataRow().withTemplate(rowTemplate,
                keyspaceMetadata.getName().asInternal(), // TABLE_SCHEM
                catalog);                                // TABLE_CATALOG
            schemas.add(row);
        }, null);

        // Results should all have the same TABLE_CATALOG, so just sort them by TABLE_SCHEM.
        schemas.sort(comparing(row -> row.getString(TABLE_SCHEMA)));
        return buildFrom(this.statement, new MetadataResultSet(rowTemplate).setRows(schemas));
    }

}
