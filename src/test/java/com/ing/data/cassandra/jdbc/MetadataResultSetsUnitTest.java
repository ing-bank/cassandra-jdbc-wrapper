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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsIterableContaining.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataResultSetsUnitTest extends UsingEmbeddedCassandraServerTest {
    private static final Logger log = LoggerFactory.getLogger(MetadataResultSetsUnitTest.class);

    private static final String KEYSPACE = "test_keyspace";
    private static final String ANOTHER_KEYSPACE = "test_keyspace2";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "version=3.0.0", "localdatacenter=datacenter1");

        // Update cluster name according to the configured name.
        try (final Statement statement = sqlConnection.createStatement()) {
            final String configuredClusterName = BuildCassandraServer.server.getNativeCluster().getClusterName();
            statement.execute("UPDATE system.local SET cluster_name = '" + configuredClusterName
                + "' WHERE key = 'local'");
        } catch (final SQLException e) {
            log.error("Cannot update cluster_name in system.local table.", e);
        }
    }

    @Test
    void givenStatement_whenMakeTableTypes_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = MetadataResultSets.instance.makeTableTypes(statement);
        assertNotNull(result);
        assertEquals(1, result.getMetaData().getColumnCount());
        assertEquals("TABLE_TYPE", result.getMetaData().getColumnName(1));
        assertEquals("TABLE", result.getString(1));
    }

    @Test
    void givenStatement_whenMakeCatalogs_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = MetadataResultSets.instance.makeCatalogs(statement);
        assertNotNull(result);
        assertEquals(1, result.getMetaData().getColumnCount());
        assertEquals("TABLE_CAT", result.getMetaData().getColumnName(1));
        assertEquals("embedded_test_cluster", result.getString(1));
    }

    @Test
    void givenStatement_whenMakeSchemas_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = MetadataResultSets.instance.makeSchemas(statement, null);
        assertNotNull(result);
        assertEquals(2, result.getMetaData().getColumnCount());
        assertEquals("TABLE_SCHEM", result.getMetaData().getColumnName(1));
        final List<String> foundKeyspaces = new ArrayList<>();
        while (result.next()) {
            foundKeyspaces.add(result.getString(1));
        }
        assertThat(foundKeyspaces, hasItem(is(KEYSPACE)));
        assertThat(foundKeyspaces, hasItem(is(ANOTHER_KEYSPACE)));
        assertEquals("TABLE_CATALOG", result.getMetaData().getColumnName(2));
        assertEquals("embedded_test_cluster", result.getString(2));

        final ResultSet result2 = MetadataResultSets.instance.makeSchemas(statement, ANOTHER_KEYSPACE);
        assertNotNull(result2);
        assertEquals(2, result2.getMetaData().getColumnCount());
        assertEquals("TABLE_SCHEM", result2.getMetaData().getColumnName(1));
        assertEquals(ANOTHER_KEYSPACE, result2.getString(1));
        assertEquals("TABLE_CATALOG", result.getMetaData().getColumnName(2));
        assertEquals("embedded_test_cluster", result.getString(2));
    }

    @Test
    void givenStatement_whenMakeTables_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = MetadataResultSets.instance.makeTables(statement, null, null);
        assertNotNull(result);
        assertEquals(10, result.getMetaData().getColumnCount());
        assertEquals("TABLE_CAT", result.getMetaData().getColumnName(1));
        assertEquals("TABLE_SCHEM", result.getMetaData().getColumnName(2));
        assertEquals("TABLE_NAME", result.getMetaData().getColumnName(3));
        assertEquals("TABLE_TYPE", result.getMetaData().getColumnName(4));
        assertEquals("REMARKS", result.getMetaData().getColumnName(5));
        assertEquals("TYPE_CAT", result.getMetaData().getColumnName(6));
        assertEquals("TYPE_SCHEM", result.getMetaData().getColumnName(7));
        assertEquals("TYPE_NAME", result.getMetaData().getColumnName(8));
        assertEquals("SELF_REFERENCING_COL_NAME", result.getMetaData().getColumnName(9));
        assertEquals("REF_GENERATION", result.getMetaData().getColumnName(10));
        final List<String> foundTables = new ArrayList<>();
        while (result.next()) {
            foundTables.add(String.join(";", result.getString(2), result.getString(3), result.getString(4),
                result.getString(5)));
        }
        assertThat(foundTables, hasItem(is(KEYSPACE.concat(";cf_test1;TABLE;First table in the keyspace"))));
        assertThat(foundTables, hasItem(is(KEYSPACE.concat(";cf_test2;TABLE;Second table in the keyspace"))));
        assertThat(foundTables, hasItem(is(ANOTHER_KEYSPACE.concat(";cf_test1;TABLE;First table in the keyspace"))));
        assertThat(foundTables, hasItem(is(ANOTHER_KEYSPACE.concat(";cf_test2;TABLE;Second table in the keyspace"))));

        int result2Size = 0;
        final ResultSet result2 = MetadataResultSets.instance.makeTables(statement, ANOTHER_KEYSPACE, null);
        assertNotNull(result2);
        foundTables.clear();
        while (result2.next()) {
            ++result2Size;
            foundTables.add(String.join(";", result2.getString(2), result2.getString(3), result2.getString(4),
                result2.getString(5)));
        }
        assertEquals(2, result2Size);
        assertThat(foundTables, hasItem(is(ANOTHER_KEYSPACE.concat(";cf_test1;TABLE;First table in the keyspace"))));
        assertThat(foundTables, hasItem(is(ANOTHER_KEYSPACE.concat(";cf_test2;TABLE;Second table in the keyspace"))));

        int result3Size = 0;
        final ResultSet result3 = MetadataResultSets.instance.makeTables(statement, null, "cf_test1");
        assertNotNull(result3);
        foundTables.clear();
        while (result3.next()) {
            ++result3Size;
            foundTables.add(String.join(";", result3.getString(2), result3.getString(3), result3.getString(4),
                result3.getString(5)));
        }
        assertEquals(2, result3Size);
        assertThat(foundTables, hasItem(is(KEYSPACE.concat(";cf_test1;TABLE;First table in the keyspace"))));
        assertThat(foundTables, hasItem(is(ANOTHER_KEYSPACE.concat(";cf_test1;TABLE;First table in the keyspace"))));

        int result4Size = 0;
        final ResultSet result4 = MetadataResultSets.instance.makeTables(statement, ANOTHER_KEYSPACE, "cf_test1");
        assertNotNull(result4);
        foundTables.clear();
        while (result4.next()) {
            ++result4Size;
            foundTables.add(String.join(";", result4.getString(2), result4.getString(3), result4.getString(4),
                result4.getString(5)));
        }
        assertEquals(1, result4Size);
        assertThat(foundTables, hasItem(is(ANOTHER_KEYSPACE.concat(";cf_test1;TABLE;First table in the keyspace"))));
    }

    @Test
    void givenStatement_whenMakeColumns_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = MetadataResultSets.instance.makeColumns(statement, KEYSPACE, "cf_test1", null);
        assertNotNull(result);
        assertEquals(24, result.getMetaData().getColumnCount());
        assertEquals("TABLE_CAT", result.getMetaData().getColumnName(1));
        assertEquals("TABLE_SCHEM", result.getMetaData().getColumnName(2));
        assertEquals("TABLE_NAME", result.getMetaData().getColumnName(3));
        assertEquals("COLUMN_NAME", result.getMetaData().getColumnName(4));
        assertEquals("DATA_TYPE", result.getMetaData().getColumnName(5));
        assertEquals("TYPE_NAME", result.getMetaData().getColumnName(6));
        assertEquals("COLUMN_SIZE", result.getMetaData().getColumnName(7));
        assertEquals("BUFFER_LENGTH", result.getMetaData().getColumnName(8));
        assertEquals("DECIMAL_DIGITS", result.getMetaData().getColumnName(9));
        assertEquals("NUM_PREC_RADIX", result.getMetaData().getColumnName(10));
        assertEquals("NULLABLE", result.getMetaData().getColumnName(11));
        assertEquals("REMARKS", result.getMetaData().getColumnName(12));
        assertEquals("COLUMN_DEF", result.getMetaData().getColumnName(13));
        assertEquals("SQL_DATA_TYPE", result.getMetaData().getColumnName(14));
        assertEquals("SQL_DATETIME_SUB", result.getMetaData().getColumnName(15));
        assertEquals("CHAR_OCTET_LENGTH", result.getMetaData().getColumnName(16));
        assertEquals("ORDINAL_POSITION", result.getMetaData().getColumnName(17));
        assertEquals("IS_NULLABLE", result.getMetaData().getColumnName(18));
        assertEquals("SCOPE_CATALOG", result.getMetaData().getColumnName(19));
        assertEquals("SCOPE_SCHEMA", result.getMetaData().getColumnName(20));
        assertEquals("SCOPE_TABLE", result.getMetaData().getColumnName(21));
        assertEquals("SOURCE_DATA_TYPE", result.getMetaData().getColumnName(22));
        assertEquals("IS_AUTOINCREMENT", result.getMetaData().getColumnName(23));
        assertEquals("IS_GENERATEDCOLUMN", result.getMetaData().getColumnName(24));
        final List<String> foundColumns = new ArrayList<>();
        int resultSize = 0;
        while (result.next()) {
            ++resultSize;
            foundColumns.add(String.join(";", result.getString(2), result.getString(3), result.getString(4),
                result.getString(6)));
        }
        assertEquals(3, resultSize);
        assertThat(foundColumns, hasItem(is(KEYSPACE.concat(";cf_test1;keyname;TEXT"))));
        assertThat(foundColumns, hasItem(is(KEYSPACE.concat(";cf_test1;t1bvalue;BOOLEAN"))));
        assertThat(foundColumns, hasItem(is(KEYSPACE.concat(";cf_test1;t1ivalue;INT"))));
    }


    @Test
    void givenStatement_whenGetResultMetadata_returnExpectedValues() throws Exception {
        final Statement stmtCreateTable = sqlConnection.createStatement();

        // Create the target table with each basic data type available in Cassandra.
        final String createTableQuery = "CREATE TABLE " + KEYSPACE + ".collections_metadata("
            + "part_key text PRIMARY KEY, "
            + "set1 set<text>, "
            + "description text, "
            + "map2 map<text,int>, "
            + "list3 list<text>);";
        stmtCreateTable.execute(createTableQuery);
        stmtCreateTable.close();

        // Insert data in the created table and select them.
        final Statement stmt = sqlConnection.createStatement();
        final String insertQuery = "INSERT INTO " + KEYSPACE
            + ".collections_metadata(part_key, set1, description, map2, list3) "
            + "VALUES ('part_key',{'val1','val2','val3'},'desc',{'val1':1,'val2':2},['val1','val2']);";
        stmt.executeQuery(insertQuery);
        final ResultSet result = stmt.executeQuery("SELECT * FROM " + KEYSPACE + ".collections_metadata");

        assertTrue(result.next());
        assertEquals(5, result.getMetaData().getColumnCount());
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            log.debug("getColumnName : " + result.getMetaData().getColumnName(i));
            log.debug("getCatalogName : " + result.getMetaData().getCatalogName(i));
            log.debug("getColumnClassName : " + result.getMetaData().getColumnClassName(i));
            log.debug("getColumnDisplaySize : " + result.getMetaData().getColumnDisplaySize(i));
            log.debug("getColumnLabel : " + result.getMetaData().getColumnLabel(i));
            log.debug("getColumnType : " + result.getMetaData().getColumnType(i));
            log.debug("getColumnTypeName : " + result.getMetaData().getColumnTypeName(i));
            log.debug("getPrecision : " + result.getMetaData().getPrecision(i));
            log.debug("getScale : " + result.getMetaData().getScale(i));
            log.debug("getSchemaName : " + result.getMetaData().getSchemaName(i));
            log.debug("getTableName : " + result.getMetaData().getTableName(i));
            log.debug("==========================");
        }

        assertEquals("part_key", result.getMetaData().getColumnName(1));
        assertEquals("description", result.getMetaData().getColumnName(2));
        assertEquals("list3", result.getMetaData().getColumnName(3));
        assertEquals("map2", result.getMetaData().getColumnName(4));
        assertEquals("set1", result.getMetaData().getColumnName(5));

        assertEquals("part_key", result.getMetaData().getColumnLabel(1));
        assertEquals("description", result.getMetaData().getColumnLabel(2));
        assertEquals("list3", result.getMetaData().getColumnLabel(3));
        assertEquals("map2", result.getMetaData().getColumnLabel(4));
        assertEquals("set1", result.getMetaData().getColumnLabel(5));

        assertEquals("collections_metadata", result.getMetaData().getTableName(1));
        assertEquals("collections_metadata", result.getMetaData().getTableName(2));
        assertEquals("collections_metadata", result.getMetaData().getTableName(3));
        assertEquals("collections_metadata", result.getMetaData().getTableName(4));
        assertEquals("collections_metadata", result.getMetaData().getTableName(5));

        assertEquals("java.lang.String", result.getMetaData().getColumnClassName(1));
        assertEquals("java.lang.String", result.getMetaData().getColumnClassName(2));
        assertEquals("java.util.List", result.getMetaData().getColumnClassName(3));
        assertEquals("java.util.Map", result.getMetaData().getColumnClassName(4));
        assertEquals("java.util.Set", result.getMetaData().getColumnClassName(5));

        assertEquals(12, result.getMetaData().getColumnType(1));
        assertEquals(12, result.getMetaData().getColumnType(2));
        assertEquals(1111, result.getMetaData().getColumnType(3));
        assertEquals(1111, result.getMetaData().getColumnType(4));
        assertEquals(1111, result.getMetaData().getColumnType(5));

        stmt.close();
    }


}
