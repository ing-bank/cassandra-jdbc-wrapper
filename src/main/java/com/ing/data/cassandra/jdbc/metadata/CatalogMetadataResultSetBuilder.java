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

/**
 * Utility class building metadata result sets ({@link CassandraMetadataResultSet} objects) related to catalogs.
 */
public class CatalogMetadataResultSetBuilder extends AbstractMetadataResultSetBuilder {

    /**
     * Constructor.
     *
     * @param statement The statement.
     * @throws SQLException if a database access error occurs or this statement is closed.
     */
    public CatalogMetadataResultSetBuilder(final CassandraStatement statement) throws SQLException {
        super(statement);
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
     * @return A valid result set for implementation of {@link DatabaseMetaData#getCatalogs()}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet buildCatalogs() throws SQLException {
        final ArrayList<MetadataRow> catalogs = new ArrayList<>();
        final MetadataRow.MetadataRowTemplate rowTemplate = new MetadataRow.MetadataRowTemplate(
            buildDefinitionInAnonymousTable(TABLE_CATALOG_SHORTNAME, DataTypes.TEXT)
        );

        catalogs.add(new MetadataRow().withTemplate(rowTemplate, this.statement.getConnection().getCatalog()));

        return buildFrom(this.statement, new MetadataResultSet(rowTemplate).setRows(catalogs));
    }

}
