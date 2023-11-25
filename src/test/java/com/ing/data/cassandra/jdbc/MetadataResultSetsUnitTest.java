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
package com.ing.data.cassandra.jdbc;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.ing.data.cassandra.jdbc.metadata.CatalogMetadataResultSetBuilder;
import com.ing.data.cassandra.jdbc.metadata.ColumnMetadataResultSetBuilder;
import com.ing.data.cassandra.jdbc.metadata.FunctionMetadataResultSetBuilder;
import com.ing.data.cassandra.jdbc.metadata.SchemaMetadataResultSetBuilder;
import com.ing.data.cassandra.jdbc.metadata.TableMetadataResultSetBuilder;
import com.ing.data.cassandra.jdbc.metadata.TypeMetadataResultSetBuilder;
import com.ing.data.cassandra.jdbc.types.DataTypeEnum;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.VECTOR;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.CASSANDRA_4;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.CASSANDRA_5;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsIterableContaining.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class MetadataResultSetsUnitTest extends UsingCassandraContainerTest {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataResultSetsUnitTest.class);

    private static final String KEYSPACE = "test_keyspace";
    private static final String ANOTHER_KEYSPACE = "test_keyspace2";
    private static final String VECTORS_KEYSPACE = "test_keyspace_vect";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
    }

    /*
     * Tables metadata
     */

    @Test
    void givenStatement_whenBuildTableTypes_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new TableMetadataResultSetBuilder(statement).buildTableTypes();
        assertNotNull(result);
        assertEquals(1, result.getMetaData().getColumnCount());
        assertEquals("TABLE_TYPE", result.getMetaData().getColumnName(1));
        assertEquals("TABLE", result.getString(1));
    }

    @Test
    void givenStatement_whenBuildTables_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new TableMetadataResultSetBuilder(statement).buildTables(null, null);
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
        final ResultSet result2 = new TableMetadataResultSetBuilder(statement).buildTables(ANOTHER_KEYSPACE, null);
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
        final ResultSet result3 = new TableMetadataResultSetBuilder(statement).buildTables(null, "cf_test1");
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
        final ResultSet result4 = new TableMetadataResultSetBuilder(statement)
            .buildTables(ANOTHER_KEYSPACE, "cf_test1");
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
    void givenStatement_whenBuildBestRowIdentifier_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new TableMetadataResultSetBuilder(statement)
            .buildBestRowIdentifier(KEYSPACE, "cf_test3", DatabaseMetaData.bestRowTemporary);
        assertNotNull(result);
        assertEquals(8, result.getMetaData().getColumnCount());
        assertEquals("SCOPE", result.getMetaData().getColumnName(1));
        assertEquals("COLUMN_NAME", result.getMetaData().getColumnName(2));
        assertEquals("DATA_TYPE", result.getMetaData().getColumnName(3));
        assertEquals("TYPE_NAME", result.getMetaData().getColumnName(4));
        assertEquals("COLUMN_SIZE", result.getMetaData().getColumnName(5));
        assertEquals("BUFFER_LENGTH", result.getMetaData().getColumnName(6));
        assertEquals("DECIMAL_DIGITS", result.getMetaData().getColumnName(7));
        assertEquals("PSEUDO_COLUMN", result.getMetaData().getColumnName(8));
        final List<String> foundColumns = new ArrayList<>();
        int resultSize = 0;
        while (result.next()) {
            ++resultSize;
            foundColumns.add(String.join(";", result.getString(1), result.getString(2), result.getString(3),
                result.getString(4), result.getString(5), result.getString(6), result.getString(7),
                result.getString(8)));
        }
        assertEquals(2, resultSize);
        assertThat(foundColumns, hasItem(is("0;keyname;12;TEXT;2147483647;0;null;1")));
        assertThat(foundColumns, hasItem(is("0;t3ivalue;4;INT;11;0;null;1")));
    }

    /*
     * Catalogs metadata
     */

    @Test
    void givenStatement_whenBuildCatalogs_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new CatalogMetadataResultSetBuilder(statement).buildCatalogs();
        assertNotNull(result);
        assertEquals(1, result.getMetaData().getColumnCount());
        assertEquals("TABLE_CAT", result.getMetaData().getColumnName(1));
        assertEquals("embedded_test_cluster", result.getString(1));
    }

    /*
     * Schemas metadata
     */

    @Test
    void givenStatement_whenBuildSchemas_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new SchemaMetadataResultSetBuilder(statement).buildSchemas(null);
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

        final ResultSet result2 = new SchemaMetadataResultSetBuilder(statement).buildSchemas(ANOTHER_KEYSPACE);
        assertNotNull(result2);
        assertEquals(2, result2.getMetaData().getColumnCount());
        assertEquals("TABLE_SCHEM", result2.getMetaData().getColumnName(1));
        assertEquals(ANOTHER_KEYSPACE, result2.getString(1));
        assertEquals("TABLE_CATALOG", result.getMetaData().getColumnName(2));
        assertEquals("embedded_test_cluster", result.getString(2));
    }

    /*
     * Columns metadata
     */

    @Test
    void givenStatement_whenBuildColumns_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new ColumnMetadataResultSetBuilder(statement).buildColumns(KEYSPACE, "cf_test1", null);
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
                result.getString(6), result.getString(7)));
        }
        assertEquals(3, resultSize);
        assertThat(foundColumns, hasItem(is(KEYSPACE.concat(";cf_test1;keyname;TEXT;2147483647"))));
        assertThat(foundColumns, hasItem(is(KEYSPACE.concat(";cf_test1;t1bvalue;BOOLEAN;5"))));
        assertThat(foundColumns, hasItem(is(KEYSPACE.concat(";cf_test1;t1ivalue;INT;11"))));
    }

    /*
     * Result sets metadata
     */

    @Test
    void givenStatement_whenGetResultMetadata_returnExpectedValues() throws Exception {
        final Statement stmtCreateTable = sqlConnection.createStatement();

        final String createTableQuery = "CREATE TABLE " + KEYSPACE + ".collections_metadata("
            + "part_key text PRIMARY KEY, "
            + "set1 set<text>, "
            + "description text, "
            + "numeric int, "
            + "map2 map<text,int>, "
            + "list3 list<text>);";
        stmtCreateTable.execute(createTableQuery);
        stmtCreateTable.close();

        // Insert data in the created table and select them.
        final Statement stmt = sqlConnection.createStatement();
        final String insertQuery = "INSERT INTO " + KEYSPACE
            + ".collections_metadata(part_key, set1, description, numeric, map2, list3) "
            + "VALUES ('part_key',{'val1','val2','val3'},'desc',100,{'val1':1,'val2':2},['val1','val2']);";
        stmt.executeQuery(insertQuery);
        final ResultSet result = stmt.executeQuery("SELECT * FROM " + KEYSPACE + ".collections_metadata");

        assertTrue(result.next());
        assertEquals(6, result.getMetaData().getColumnCount());
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            LOG.debug("getColumnName : {}", result.getMetaData().getColumnName(i));
            LOG.debug("getCatalogName : {}", result.getMetaData().getCatalogName(i));
            LOG.debug("getColumnClassName : {}", result.getMetaData().getColumnClassName(i));
            LOG.debug("getColumnDisplaySize : {}", result.getMetaData().getColumnDisplaySize(i));
            LOG.debug("getColumnLabel : {}", result.getMetaData().getColumnLabel(i));
            LOG.debug("getColumnType : {}", result.getMetaData().getColumnType(i));
            LOG.debug("getColumnTypeName : {}", result.getMetaData().getColumnTypeName(i));
            LOG.debug("getPrecision : {}", result.getMetaData().getPrecision(i));
            LOG.debug("getScale : {}", result.getMetaData().getScale(i));
            LOG.debug("getSchemaName : {}", result.getMetaData().getSchemaName(i));
            LOG.debug("getTableName : {}", result.getMetaData().getTableName(i));
            LOG.debug("==========================");
        }

        assertEquals("part_key", result.getMetaData().getColumnName(1));
        assertEquals("description", result.getMetaData().getColumnName(2));
        assertEquals("list3", result.getMetaData().getColumnName(3));
        assertEquals("map2", result.getMetaData().getColumnName(4));
        assertEquals("numeric", result.getMetaData().getColumnName(5));
        assertEquals("set1", result.getMetaData().getColumnName(6));

        assertEquals("part_key", result.getMetaData().getColumnLabel(1));
        assertEquals("description", result.getMetaData().getColumnLabel(2));
        assertEquals("list3", result.getMetaData().getColumnLabel(3));
        assertEquals("map2", result.getMetaData().getColumnLabel(4));
        assertEquals("numeric", result.getMetaData().getColumnLabel(5));
        assertEquals("set1", result.getMetaData().getColumnLabel(6));

        assertEquals("collections_metadata", result.getMetaData().getTableName(1));
        assertEquals("collections_metadata", result.getMetaData().getTableName(2));
        assertEquals("collections_metadata", result.getMetaData().getTableName(3));
        assertEquals("collections_metadata", result.getMetaData().getTableName(4));
        assertEquals("collections_metadata", result.getMetaData().getTableName(5));
        assertEquals("collections_metadata", result.getMetaData().getTableName(6));

        assertEquals("java.lang.String", result.getMetaData().getColumnClassName(1));
        assertEquals("java.lang.String", result.getMetaData().getColumnClassName(2));
        assertEquals("java.util.List", result.getMetaData().getColumnClassName(3));
        assertEquals("java.util.Map", result.getMetaData().getColumnClassName(4));
        assertEquals("java.lang.Integer", result.getMetaData().getColumnClassName(5));
        assertEquals("java.util.Set", result.getMetaData().getColumnClassName(6));

        assertEquals(12, result.getMetaData().getColumnType(1));
        assertEquals(12, result.getMetaData().getColumnType(2));
        assertEquals(1111, result.getMetaData().getColumnType(3));
        assertEquals(1111, result.getMetaData().getColumnType(4));
        assertEquals(4, result.getMetaData().getColumnType(5));
        assertEquals(1111, result.getMetaData().getColumnType(6));

        assertFalse(result.getMetaData().isSigned(1));
        assertFalse(result.getMetaData().isSigned(2));
        assertFalse(result.getMetaData().isSigned(3));
        assertFalse(result.getMetaData().isSigned(4));
        assertTrue(result.getMetaData().isSigned(5));
        assertFalse(result.getMetaData().isSigned(6));

        stmt.close();
    }

    @Test
    void givenCassandraMetadataResultSet_whenUnwrap_returnUnwrappedMetadataResultSet() {
        try (final CassandraMetadataResultSet metadataRs = new CassandraMetadataResultSet()) {
            assertNotNull(metadataRs.unwrap(ResultSet.class));
            assertNotNull(metadataRs.unwrap(CassandraResultSetExtras.class));
        } catch (final Exception e) {
            fail(e);
        }
    }

    @Test
    void givenCassandraMetadataResultSet_whenUnwrapToInvalidInterface_throwException() {
        try (final CassandraMetadataResultSet metadataRs = new CassandraMetadataResultSet()) {
            assertThrows(SQLException.class, () -> metadataRs.unwrap(this.getClass()));
        } catch (final Exception e) {
            fail(e);
        }
    }

    @Test
    void givenStatement_whenGetMetadataIsSearchable_returnExpectedValues() throws Exception {
        final Statement stmtCreateTable = sqlConnection.createStatement();

        final String createTableQuery = "CREATE TABLE " + KEYSPACE + ".searchable_metadata("
            + "part_key_1 text, "
            + "part_key_2 text, "
            + "clust_col text, "
            + "text_col text, "
            + "indexed_col text, "
            + "PRIMARY KEY ((part_key_1, part_key_2), clust_col));";
        stmtCreateTable.execute(createTableQuery);
        final String createIndexQuery = "CREATE INDEX idx_test ON " + KEYSPACE + ".searchable_metadata (indexed_col);";
        stmtCreateTable.execute(createIndexQuery);
        stmtCreateTable.close();

        // Insert data in the created table and select them.
        final Statement stmt = sqlConnection.createStatement();
        final String insertQuery = "INSERT INTO " + KEYSPACE
            + ".searchable_metadata(part_key_1, part_key_2, clust_col, text_col, indexed_col) "
            + "VALUES ('part_key1', 'part_key2', 'clustering_column', 'any text', 'indexed_column');";
        stmt.executeQuery(insertQuery);
        final ResultSet result = stmt.executeQuery("SELECT part_key_1, part_key_2, clust_col, text_col, indexed_col "
            + "FROM " + KEYSPACE + ".searchable_metadata");

        assertTrue(result.next());
        assertTrue(result.getMetaData().isSearchable(1));
        assertTrue(result.getMetaData().isSearchable(2));
        assertTrue(result.getMetaData().isSearchable(3));
        assertFalse(result.getMetaData().isSearchable(4));
        assertTrue(result.getMetaData().isSearchable(5));

        stmt.close();
    }

    @Test
    void givenMetadataResultSet_whenFindColumns_returnExpectedIndex() throws Exception {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final CassandraMetadataResultSet metadataResultSet =
            new TableMetadataResultSetBuilder(statement).buildTables(KEYSPACE, "cf_test1");
        assertEquals(3, metadataResultSet.findColumn("TABLE_NAME"));
        final SQLSyntaxErrorException exception = assertThrows(SQLSyntaxErrorException.class,
            () -> metadataResultSet.findColumn("CATALOG"));
        assertEquals("Name provided was not in the list of valid column labels: CATALOG", exception.getMessage());
    }

    @Test
    void givenIncompleteMetadataResultSet_whenFindColumns_throwException() {
        final CassandraMetadataResultSet metadataResultSet = new CassandraMetadataResultSet();
        final SQLSyntaxErrorException exception = assertThrows(SQLSyntaxErrorException.class,
            () -> metadataResultSet.findColumn("COLUMN_NAME"));
        assertEquals("Name provided was not in the list of valid column labels: COLUMN_NAME", exception.getMessage());
    }

    /*
     * Types metadata
     */

    @Test
    void givenStatement_whenBuildUDTs_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new TypeMetadataResultSetBuilder(statement).buildUDTs(KEYSPACE, "CustomType1",
            new int[]{Types.JAVA_OBJECT});
        assertNotNull(result);
        assertEquals(7, result.getMetaData().getColumnCount());
        assertEquals("TYPE_CAT", result.getMetaData().getColumnName(1));
        assertEquals("TYPE_SCHEM", result.getMetaData().getColumnName(2));
        assertEquals("TYPE_NAME", result.getMetaData().getColumnName(3));
        assertEquals("CLASS_NAME", result.getMetaData().getColumnName(4));
        assertEquals("DATA_TYPE", result.getMetaData().getColumnName(5));
        assertEquals("REMARKS", result.getMetaData().getColumnName(6));
        assertEquals("BASE_TYPE", result.getMetaData().getColumnName(7));
        final List<String> foundColumns = new ArrayList<>();
        int resultSize = 0;
        while (result.next()) {
            ++resultSize;
            foundColumns.add(String.join(";", result.getString(2), result.getString(3), result.getString(4),
                result.getString(5), result.getString(6), result.getString(7)));
        }
        assertEquals(1, resultSize);
        assertThat(foundColumns, hasItem(is(KEYSPACE.concat(";customtype1;").concat(UdtValue.class.getName())
            .concat(";2000;;null"))));

        // Using a fully-qualified type name.
        final ResultSet resultFullyQualifiedName = new TypeMetadataResultSetBuilder(statement)
            .buildUDTs(KEYSPACE, KEYSPACE + ".customtype2", new int[]{Types.JAVA_OBJECT});
        assertNotNull(resultFullyQualifiedName);
        assertEquals(7, resultFullyQualifiedName.getMetaData().getColumnCount());
        assertTrue(resultFullyQualifiedName.next());
    }

    @Test
    void givenStatement_whenBuildUDTsWithNonJavaObjectTypes_returnEmptyResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new TypeMetadataResultSetBuilder(statement).buildUDTs(KEYSPACE, "CustomType1",
            new int[]{Types.STRUCT, Types.DISTINCT});
        assertNotNull(result);
        assertFalse(result.next());
    }

    @Test
    void givenStatement_whenBuildUDTsNotSpecifyingTypeNamePattern_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new TypeMetadataResultSetBuilder(statement).buildUDTs(KEYSPACE, null,
            new int[]{Types.JAVA_OBJECT});
        assertNotNull(result);
        final List<String> foundColumns = new ArrayList<>();
        int resultSize = 0;
        while (result.next()) {
            ++resultSize;
            foundColumns.add(String.join(";", result.getString(2), result.getString(3)));
        }
        assertEquals(3, resultSize);
        assertEquals(KEYSPACE.concat(";customtype1"), foundColumns.get(0));
        assertEquals(KEYSPACE.concat(";customtype2"), foundColumns.get(1));
        assertEquals(KEYSPACE.concat(";type_in_different_ks"), foundColumns.get(2));
    }

    @Test
    void givenStatement_whenBuildUDTsNotSpecifyingSchemaPattern_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new TypeMetadataResultSetBuilder(statement).buildUDTs(null, "type_in_different_ks",
            new int[]{Types.JAVA_OBJECT});
        assertNotNull(result);
        assertEquals(7, result.getMetaData().getColumnCount());
        final List<String> foundColumns = new ArrayList<>();
        int resultSize = 0;
        while (result.next()) {
            ++resultSize;
            foundColumns.add(String.join(";", result.getString(2), result.getString(3)));
        }
        assertEquals(2, resultSize);
        assertEquals(KEYSPACE.concat(";type_in_different_ks"), foundColumns.get(0));
        assertEquals(ANOTHER_KEYSPACE.concat(";type_in_different_ks"), foundColumns.get(1));
    }

    static Stream<Arguments> buildCqlTypesTestCases() {
        return Stream.of(
            Arguments.of(CASSANDRA_5, DataTypeEnum.values().length),
            Arguments.of(CASSANDRA_4, DataTypeEnum.values().length - 1) // Type VECTOR appears in Cassandra 5.0
        );
    }

    @ParameterizedTest
    @MethodSource("buildCqlTypesTestCases")
    void givenStatement_whenBuildTypes_returnExpectedResultSet(final String dbVersion, final int expectedNbOfTypes)
        throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new TypeMetadataResultSetBuilder(statement).buildTypes(dbVersion);
        assertNotNull(result);
        assertEquals(18, result.getMetaData().getColumnCount());
        assertEquals("TYPE_NAME", result.getMetaData().getColumnName(1));
        assertEquals("DATA_TYPE", result.getMetaData().getColumnName(2));
        assertEquals("PRECISION", result.getMetaData().getColumnName(3));
        assertEquals("LITERAL_PREFIX", result.getMetaData().getColumnName(4));
        assertEquals("LITERAL_SUFFIX", result.getMetaData().getColumnName(5));
        assertEquals("CREATE_PARAMS", result.getMetaData().getColumnName(6));
        assertEquals("NULLABLE", result.getMetaData().getColumnName(7));
        assertEquals("CASE_SENSITIVE", result.getMetaData().getColumnName(8));
        assertEquals("SEARCHABLE", result.getMetaData().getColumnName(9));
        assertEquals("UNSIGNED_ATTRIBUTE", result.getMetaData().getColumnName(10));
        assertEquals("FIXED_PREC_SCALE", result.getMetaData().getColumnName(11));
        assertEquals("AUTO_INCREMENT", result.getMetaData().getColumnName(12));
        assertEquals("LOCAL_TYPE_NAME", result.getMetaData().getColumnName(13));
        assertEquals("MINIMUM_SCALE", result.getMetaData().getColumnName(14));
        assertEquals("MAXIMUM_SCALE", result.getMetaData().getColumnName(15));
        assertEquals("SQL_DATA_TYPE", result.getMetaData().getColumnName(16));
        assertEquals("SQL_DATETIME_SUB", result.getMetaData().getColumnName(17));
        assertEquals("NUM_PREC_RADIX", result.getMetaData().getColumnName(18));
        final List<String> foundColumns = new ArrayList<>();
        int resultSize = 0;
        while (result.next()) {
            ++resultSize;
            List<String> results = new ArrayList<>();
            for (int i = 1; i <= 18; i++) {
                results.add(result.getString(i));
            }
            foundColumns.add(String.join(";", results));
        }
        assertEquals(expectedNbOfTypes, resultSize);
        assertEquals("tinyint;-6;4;null;null;null;1;false;2;false;true;false;null;0;0;null;null;4",
            foundColumns.get(0));
        assertEquals("bigint;-5;20;null;null;null;1;false;2;false;true;false;null;0;0;null;null;20",
            foundColumns.get(1));
        assertEquals("counter;-5;20;null;null;null;1;false;2;false;true;false;null;0;0;null;null;20",
            foundColumns.get(2));
        assertEquals("varint;-5;20;null;null;null;1;false;2;false;true;false;null;0;0;null;null;20",
            foundColumns.get(3));
        assertEquals("blob;-2;1073741823;';';null;1;false;2;true;true;false;null;0;0;null;null;1073741823",
            foundColumns.get(4));
        assertEquals("decimal;3;0;null;null;null;1;false;2;false;true;false;null;0;0;null;null;0",
            foundColumns.get(5));
        assertEquals("int;4;11;null;null;null;1;false;2;false;true;false;null;0;0;null;null;11",
            foundColumns.get(6));
        assertEquals("smallint;5;6;null;null;null;1;false;2;false;true;false;null;0;0;null;null;6",
            foundColumns.get(7));
        assertEquals("float;6;7;null;null;null;1;false;2;false;true;false;null;0;40;null;null;7",
            foundColumns.get(8));
        assertEquals("double;8;300;null;null;null;1;false;2;false;true;false;null;0;300;null;null;300",
            foundColumns.get(9));
        assertEquals("ascii;12;2147483647;';';null;1;true;2;true;true;false;null;0;0;null;null;2147483647",
            foundColumns.get(10));
        assertEquals("text;12;2147483647;';';null;1;true;2;true;true;false;null;0;0;null;null;2147483647",
            foundColumns.get(11));
        assertEquals("VARCHAR;12;2147483647;';';null;1;true;2;true;true;false;null;0;0;null;null;2147483647",
            foundColumns.get(12));
        assertEquals("boolean;16;5;null;null;null;1;false;2;true;true;false;null;0;0;null;null;5",
            foundColumns.get(13));
        assertEquals("date;91;10;null;null;null;1;false;2;true;true;false;null;0;0;null;null;10",
            foundColumns.get(14));
        assertEquals("time;92;18;null;null;null;1;false;2;true;true;false;null;0;0;null;null;18",
            foundColumns.get(15));
        assertEquals("timestamp;93;31;null;null;null;1;false;2;true;true;false;null;0;0;null;null;31",
            foundColumns.get(16));
        assertEquals("CUSTOM;1111;-1;';';null;1;true;2;true;true;false;null;0;0;null;null;-1",
            foundColumns.get(17));
        assertEquals("duration;1111;-1;null;null;null;1;false;2;true;true;false;null;0;0;null;null;-1",
            foundColumns.get(18));
        assertEquals("inet;1111;39;null;null;null;1;false;2;false;true;false;null;0;0;null;null;39",
            foundColumns.get(19));
        assertEquals("list;1111;-1;null;null;null;1;false;2;true;true;false;null;0;0;null;null;-1",
            foundColumns.get(20));
        assertEquals("map;1111;-1;null;null;null;1;false;2;true;true;false;null;0;0;null;null;-1",
            foundColumns.get(21));
        assertEquals("set;1111;-1;null;null;null;1;false;2;true;true;false;null;0;0;null;null;-1",
            foundColumns.get(22));
        assertEquals("timeuuid;1111;36;null;null;null;1;false;2;true;true;false;null;0;0;null;null;36",
            foundColumns.get(23));
        assertEquals("tuple;1111;-1;';';null;1;true;2;true;true;false;null;0;0;null;null;-1",
            foundColumns.get(24));
        assertEquals("UDT;1111;-1;';';null;1;true;2;true;true;false;null;0;0;null;null;-1",
            foundColumns.get(25));
        assertEquals("uuid;1111;36;null;null;null;1;false;2;true;true;false;null;0;0;null;null;36",
            foundColumns.get(26));
        if (CASSANDRA_5.equals(dbVersion)) {
            assertEquals(VECTOR.cqlType
                    .concat(";1111;-1;null;null;null;1;false;2;true;true;false;null;0;0;null;null;-1"),
                foundColumns.get(27));
        }
    }

    @Test
    void givenStatement_whenBuildAttributes_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new TypeMetadataResultSetBuilder(statement)
            .buildAttributes(KEYSPACE, "type_in_different_ks", "t_%");
        assertNotNull(result);
        assertEquals(21, result.getMetaData().getColumnCount());
        assertEquals("TYPE_CAT", result.getMetaData().getColumnName(1));
        assertEquals("TYPE_SCHEM", result.getMetaData().getColumnName(2));
        assertEquals("TYPE_NAME", result.getMetaData().getColumnName(3));
        assertEquals("ATTR_NAME", result.getMetaData().getColumnName(4));
        assertEquals("DATA_TYPE", result.getMetaData().getColumnName(5));
        assertEquals("ATTR_TYPE_NAME", result.getMetaData().getColumnName(6));
        assertEquals("ATTR_SIZE", result.getMetaData().getColumnName(7));
        assertEquals("DECIMAL_DIGITS", result.getMetaData().getColumnName(8));
        assertEquals("NUM_PREC_RADIX", result.getMetaData().getColumnName(9));
        assertEquals("NULLABLE", result.getMetaData().getColumnName(10));
        assertEquals("REMARKS", result.getMetaData().getColumnName(11));
        assertEquals("ATTR_DEF", result.getMetaData().getColumnName(12));
        assertEquals("SQL_DATA_TYPE", result.getMetaData().getColumnName(13));
        assertEquals("SQL_DATETIME_SUB", result.getMetaData().getColumnName(14));
        assertEquals("CHAR_OCTET_LENGTH", result.getMetaData().getColumnName(15));
        assertEquals("ORDINAL_POSITION", result.getMetaData().getColumnName(16));
        assertEquals("IS_NULLABLE", result.getMetaData().getColumnName(17));
        assertEquals("SCOPE_CATALOG", result.getMetaData().getColumnName(18));
        assertEquals("SCOPE_SCHEMA", result.getMetaData().getColumnName(19));
        assertEquals("SCOPE_TABLE", result.getMetaData().getColumnName(20));
        assertEquals("SOURCE_DATA_TYPE", result.getMetaData().getColumnName(21));
        final List<String> foundAttrs = new ArrayList<>();
        int resultSize = 0;
        while (result.next()) {
            ++resultSize;
            foundAttrs.add(String.join(";", result.getString(2), result.getString(3), result.getString(4),
                result.getString(6), result.getString(16)));
        }
        assertEquals(2, resultSize);
        assertThat(foundAttrs, hasItem(is(KEYSPACE.concat(";type_in_different_ks;t_key;INT;1"))));
        assertThat(foundAttrs, hasItem(is(KEYSPACE.concat(";type_in_different_ks;t_value;TEXT;2"))));
    }

    /*
     * Functions metadata
     */

    @Test
    void givenStatement_whenBuildFunctions_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new FunctionMetadataResultSetBuilder(statement)
            .buildFunctions(KEYSPACE, "function_test1");
        assertNotNull(result);
        assertEquals(6, result.getMetaData().getColumnCount());
        assertEquals("FUNCTION_CAT", result.getMetaData().getColumnName(1));
        assertEquals("FUNCTION_SCHEM", result.getMetaData().getColumnName(2));
        assertEquals("FUNCTION_NAME", result.getMetaData().getColumnName(3));
        assertEquals("REMARKS", result.getMetaData().getColumnName(4));
        assertEquals("FUNCTION_TYPE", result.getMetaData().getColumnName(5));
        assertEquals("SPECIFIC_NAME", result.getMetaData().getColumnName(6));
        final List<String> foundColumns = new ArrayList<>();
        int resultSize = 0;
        while (result.next()) {
            ++resultSize;
            foundColumns.add(String.join(";", result.getString(2), result.getString(3), result.getString(4),
                result.getString(5), result.getString(6)));
        }
        assertEquals(1, resultSize);
        assertThat(foundColumns, hasItem(is(KEYSPACE.concat(";function_test1;;1;function_test1"))));
    }

    @Test
    void givenStatement_whenBuildFunctionColumns_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        final ResultSet result = new FunctionMetadataResultSetBuilder(statement)
            .buildFunctionColumns(KEYSPACE, "function_test1", "%");
        assertNotNull(result);
        assertEquals(17, result.getMetaData().getColumnCount());
        assertEquals("FUNCTION_CAT", result.getMetaData().getColumnName(1));
        assertEquals("FUNCTION_SCHEM", result.getMetaData().getColumnName(2));
        assertEquals("FUNCTION_NAME", result.getMetaData().getColumnName(3));
        assertEquals("COLUMN_NAME", result.getMetaData().getColumnName(4));
        assertEquals("COLUMN_TYPE", result.getMetaData().getColumnName(5));
        assertEquals("DATA_TYPE", result.getMetaData().getColumnName(6));
        assertEquals("TYPE_NAME", result.getMetaData().getColumnName(7));
        assertEquals("PRECISION", result.getMetaData().getColumnName(8));
        assertEquals("LENGTH", result.getMetaData().getColumnName(9));
        assertEquals("SCALE", result.getMetaData().getColumnName(10));
        assertEquals("RADIX", result.getMetaData().getColumnName(11));
        assertEquals("NULLABLE", result.getMetaData().getColumnName(12));
        assertEquals("REMARKS", result.getMetaData().getColumnName(13));
        assertEquals("CHAR_OCTET_LENGTH", result.getMetaData().getColumnName(14));
        assertEquals("ORDINAL_POSITION", result.getMetaData().getColumnName(15));
        assertEquals("IS_NULLABLE", result.getMetaData().getColumnName(16));
        assertEquals("SPECIFIC_NAME", result.getMetaData().getColumnName(17));

        final List<String> foundColumns = new ArrayList<>();
        int resultSize = 0;
        while (result.next()) {
            ++resultSize;
            List<String> results = new ArrayList<>();
            for (int i = 2; i <= 17; i++) {
                results.add(result.getString(i));
            }
            foundColumns.add(String.join(";", results));
        }
        assertEquals(3, resultSize);
        assertEquals(KEYSPACE.concat(
            ";function_test1;;4;4;INT;11;2147483647;0;11;1;;null;0;YES;function_test1"), foundColumns.get(0));
        assertEquals(KEYSPACE.concat(
            ";function_test1;var1;1;4;INT;11;2147483647;0;11;1;;null;1;YES;function_test1"),
            foundColumns.get(1));
        assertEquals(KEYSPACE.concat(
            ";function_test1;var2;1;12;TEXT;2147483647;2147483647;0;2147483647;1;;null;2;YES;function_test1"),
            foundColumns.get(2));
    }

    /*
     * Indexes metadata
     */

    @Test
    void givenStatement_whenBuildIndexes_returnExpectedResultSet() throws SQLException {
        final CassandraStatement statement = (CassandraStatement) sqlConnection.createStatement();
        ResultSet result = new TableMetadataResultSetBuilder(statement)
            .buildIndexes(VECTORS_KEYSPACE, "pet_supply_vectors", false, false);
        assertNotNull(result);
        assertEquals(13, result.getMetaData().getColumnCount());
        assertEquals("TABLE_CAT", result.getMetaData().getColumnName(1));
        assertEquals("TABLE_SCHEM", result.getMetaData().getColumnName(2));
        assertEquals("TABLE_NAME", result.getMetaData().getColumnName(3));
        assertEquals("NON_UNIQUE", result.getMetaData().getColumnName(4));
        assertEquals("INDEX_QUALIFIER", result.getMetaData().getColumnName(5));
        assertEquals("INDEX_NAME", result.getMetaData().getColumnName(6));
        assertEquals("TYPE", result.getMetaData().getColumnName(7));
        assertEquals("ORDINAL_POSITION", result.getMetaData().getColumnName(8));
        assertEquals("COLUMN_NAME", result.getMetaData().getColumnName(9));
        assertEquals("ASC_OR_DESC", result.getMetaData().getColumnName(10));
        assertEquals("CARDINALITY", result.getMetaData().getColumnName(11));
        assertEquals("PAGES", result.getMetaData().getColumnName(12));
        assertEquals("FILTER_CONDITION", result.getMetaData().getColumnName(13));
        final List<String> foundColumns = new ArrayList<>();
        int resultSize = 0;
        while (result.next()) {
            ++resultSize;
            foundColumns.add(String.join(";", result.getString(2), result.getString(3), result.getString(4),
                result.getString(6), result.getString(7), result.getString(8), result.getString(9)));
        }
        assertEquals(1, resultSize);
        assertThat(foundColumns,
            hasItem(is(VECTORS_KEYSPACE.concat(";pet_supply_vectors;true;idx_vector;3;1;product_vector"))));

        result = new TableMetadataResultSetBuilder(statement).buildIndexes(ANOTHER_KEYSPACE, "cf_test2", false, false);
        assertNotNull(result);
        foundColumns.clear();
        resultSize = 0;
        while (result.next()) {
            ++resultSize;
            foundColumns.add(String.join(";", result.getString(2), result.getString(3), result.getString(4),
                result.getString(6), result.getString(7), result.getString(8), result.getString(9)));
        }
        assertEquals(1, resultSize);
        assertThat(foundColumns, hasItem(is(ANOTHER_KEYSPACE.concat(";cf_test2;true;int_values_idx;3;1;t2ivalue"))));
    }

}
