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

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MetadataResultSet {

    private ArrayList<MetadataRow> rows;

    /**
     * Constructor.
     */
    public MetadataResultSet() {
    }

    public MetadataResultSet setRows(final ArrayList<MetadataRow> schemas) {
        this.rows = schemas;
        return this;
    }

    public ColumnDefinitions getColumnDefinitions() {
        return null;
    }

    public boolean isExhausted() {
        return false;
    }

    public Row one() {
        return null;
    }

    public List<MetadataRow> all() {
        return rows;
    }

    public Iterator<MetadataRow> iterator() {
        return rows.iterator();
    }

    public int getAvailableWithoutFetching() {
        return 0;
    }

    public boolean isFullyFetched() {
        return false;
    }

    public ListenableFuture<Void> fetchMoreResults() {
        return null;
    }

    public ExecutionInfo getExecutionInfo() {
        return null;
    }

    public List<ExecutionInfo> getAllExecutionInfo() {
        return null;
    }

    public boolean wasApplied() {
        return false;
    }

}
