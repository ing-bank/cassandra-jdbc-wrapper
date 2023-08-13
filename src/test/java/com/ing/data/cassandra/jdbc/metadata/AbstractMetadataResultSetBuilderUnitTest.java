/*
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
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ing.data.cassandra.jdbc.CassandraConnection;
import com.ing.data.cassandra.jdbc.CassandraStatement;
import com.ing.data.cassandra.jdbc.utils.TestMetadataResultSetBuilder;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractMetadataResultSetBuilderUnitTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractMetadataResultSetBuilderUnitTest.class);

    static KeyspaceMetadata generateTestKeyspaceMetadata(final String keyspaceName) {
        final KeyspaceMetadata mockKeyspaceMetadata = mock(KeyspaceMetadata.class);
        when(mockKeyspaceMetadata.getName()).thenReturn(CqlIdentifier.fromCql(keyspaceName));
        return mockKeyspaceMetadata;
    }

    static TableMetadata generateTestTableMetadata(final String tableName) {
        final TableMetadata mockTableMetadata = mock(TableMetadata.class);
        when(mockTableMetadata.getName()).thenReturn(CqlIdentifier.fromCql(tableName));
        return mockTableMetadata;
    }

    @Test
    void givenSchemaPattern_whenApplySchemaFiltering_returnExpectedResultSet() throws SQLException {
        final CassandraStatement mockStatement = mock(CassandraStatement.class);
        final CassandraConnection mockConnection = mock(CassandraConnection.class);
        final Metadata mockMetadata = mock(Metadata.class);
        when(mockStatement.getCassandraConnection()).thenReturn(mockConnection);
        when(mockConnection.getClusterMetadata()).thenReturn(mockMetadata);
        final Map<CqlIdentifier, KeyspaceMetadata> testKeyspacesMetadata = new HashMap<>();
        testKeyspacesMetadata.put(CqlIdentifier.fromInternal("ks1"), generateTestKeyspaceMetadata("ks1"));
        testKeyspacesMetadata.put(CqlIdentifier.fromInternal("ks2"), generateTestKeyspaceMetadata("ks2"));
        testKeyspacesMetadata.put(CqlIdentifier.fromInternal("test_ks"), generateTestKeyspaceMetadata("test_ks"));
        testKeyspacesMetadata.put(CqlIdentifier.fromInternal("another"),generateTestKeyspaceMetadata("another"));
        when(mockMetadata.getKeyspaces()).thenReturn(testKeyspacesMetadata);
        final AbstractMetadataResultSetBuilder sut = new TestMetadataResultSetBuilder(mockStatement);

        final Set<String> filteredSchemas = new HashSet<>();
        sut.filterBySchemaNamePattern(StringUtils.EMPTY,
            keyspaceMetadata -> filteredSchemas.add(keyspaceMetadata.getName().asInternal()));
        log.info("Schemas matching '': {}", filteredSchemas);
        assertThat(filteredSchemas, hasSize(4));
        assertThat(filteredSchemas, hasItems("ks1", "ks2", "test_ks", "another"));

        filteredSchemas.clear();
        sut.filterBySchemaNamePattern(null,
            keyspaceMetadata -> filteredSchemas.add(keyspaceMetadata.getName().asInternal()));
        log.info("Schemas matching null: {}", filteredSchemas);
        assertThat(filteredSchemas, hasSize(4));
        assertThat(filteredSchemas, hasItems("ks1", "ks2", "test_ks", "another"));

        filteredSchemas.clear();
        sut.filterBySchemaNamePattern("ks",
            keyspaceMetadata -> filteredSchemas.add(keyspaceMetadata.getName().asInternal()));
        log.info("Schemas matching 'ks': {}", filteredSchemas);
        assertThat(filteredSchemas, hasSize(0));

        filteredSchemas.clear();
        sut.filterBySchemaNamePattern("ks%",
            keyspaceMetadata -> filteredSchemas.add(keyspaceMetadata.getName().asInternal()));
        log.info("Schemas matching 'ks%': {}", filteredSchemas);
        assertThat(filteredSchemas, hasSize(2));
        assertThat(filteredSchemas, hasItems("ks1", "ks2"));

        filteredSchemas.clear();
        sut.filterBySchemaNamePattern("%ks%",
            keyspaceMetadata -> filteredSchemas.add(keyspaceMetadata.getName().asInternal()));
        log.info("Schemas matching '%ks%': {}", filteredSchemas);
        assertThat(filteredSchemas, hasSize(3));
        assertThat(filteredSchemas, hasItems("ks1", "ks2", "test_ks"));
    }

    @Test
    void givenTablePattern_whenApplyTableFiltering_returnExpectedResultSet() throws SQLException {
        final CassandraStatement mockStatement = mock(CassandraStatement.class);
        final CassandraConnection mockConnection = mock(CassandraConnection.class);
        final Metadata mockMetadata = mock(Metadata.class);
        when(mockStatement.getCassandraConnection()).thenReturn(mockConnection);
        when(mockConnection.getClusterMetadata()).thenReturn(mockMetadata);
        final KeyspaceMetadata ksTestMetadata = generateTestKeyspaceMetadata("ks_test");
        final Map<CqlIdentifier, TableMetadata> testTablesMetadata = new HashMap<>();
        testTablesMetadata.put(CqlIdentifier.fromInternal("cf1"), generateTestTableMetadata("cf1"));
        testTablesMetadata.put(CqlIdentifier.fromInternal("cf2"), generateTestTableMetadata("cf2"));
        testTablesMetadata.put(CqlIdentifier.fromInternal("another_table"), generateTestTableMetadata("another_table"));
        testTablesMetadata.put(CqlIdentifier.fromInternal("test_cf"), generateTestTableMetadata("test_cf"));
        when(ksTestMetadata.getTables()).thenReturn(testTablesMetadata);
        final AbstractMetadataResultSetBuilder sut = new TestMetadataResultSetBuilder(mockStatement);

        final Set<String> filteredTables = new HashSet<>();
        sut.filterByTableNamePattern(StringUtils.EMPTY, ksTestMetadata,
            tableMetadata -> filteredTables.add(tableMetadata.getName().asInternal()));
        log.info("Tables matching '': {}", filteredTables);
        assertThat(filteredTables, empty());

        filteredTables.clear();
        sut.filterByTableNamePattern(null, ksTestMetadata,
            tableMetadata -> filteredTables.add(tableMetadata.getName().asInternal()));
        log.info("Tables matching null: {}", filteredTables);
        assertThat(filteredTables, hasSize(4));
        assertThat(filteredTables, hasItems("cf1", "cf2", "another_table", "test_cf"));

        filteredTables.clear();
        sut.filterByTableNamePattern("cf", ksTestMetadata,
            tableMetadata -> filteredTables.add(tableMetadata.getName().asInternal()));
        log.info("Tables matching 'cf': {}", filteredTables);
        assertThat(filteredTables, empty());

        filteredTables.clear();
        sut.filterByTableNamePattern("cf%", ksTestMetadata,
            tableMetadata -> filteredTables.add(tableMetadata.getName().asInternal()));
        log.info("Tables matching 'cf%': {}", filteredTables);
        assertThat(filteredTables, hasSize(2));
        assertThat(filteredTables, hasItems("cf1", "cf2"));

        filteredTables.clear();
        sut.filterByTableNamePattern("%cf%", ksTestMetadata,
            tableMetadata -> filteredTables.add(tableMetadata.getName().asInternal()));
        log.info("Tables matching '%cf%': {}", filteredTables);
        assertThat(filteredTables, hasSize(3));
        assertThat(filteredTables, hasItems("cf1", "cf2", "test_cf"));
    }
}
