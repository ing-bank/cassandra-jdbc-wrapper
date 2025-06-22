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

import com.ing.data.cassandra.jdbc.ColumnDefinitions;

import java.util.Iterator;
import java.util.List;

/**
 * A simple metadata result set made of {@link MetadataRow} objects.
 *
 * @see AbstractMetadataResultSetBuilder
 */
public class MetadataResultSet {

    private List<MetadataRow> rows;
    private ColumnDefinitions columnDefinitions;

    /**
     * Constructor.
     */
    public MetadataResultSet() {
    }

    /**
     * Constructor including the columns definitions from a metadata row template.
     *
     * @param rowTemplate The metadata row template from which the columns definitions of the metadata result set
     *                    are extracted.
     */
    public MetadataResultSet(final MetadataRow.MetadataRowTemplate rowTemplate) {
        this.columnDefinitions = new ColumnDefinitions(rowTemplate.getColumnDefinitions());
    }

    /**
     * Add rows to the metadata result set.
     *
     * @param metadataRows A list of {@code MetadataRows}.
     * @return The updated instance of {@code MetadataResultSet}.
     */
    public MetadataResultSet setRows(final List<MetadataRow> metadataRows) {
        this.rows = metadataRows;
        // If there is at least one row, use the columns definitions of the first one as columns definitions for all
        // the rows.
        if (!metadataRows.isEmpty()) {
            this.columnDefinitions = metadataRows.get(0).getColumnDefinitions();
        }
        return this;
    }

    /**
     * Gets the columns of the metadata result set.
     *
     * @return The columns of the metadata result set.
     */
    public ColumnDefinitions getColumnDefinitions() {
        return this.columnDefinitions;
    }

    /**
     * Gets an iterator over the rows of the metadata result set.
     *
     * @return An iterator over the rows of the metadata result set.
     */
    public Iterator<MetadataRow> iterator() {
        return this.rows.iterator();
    }

}
