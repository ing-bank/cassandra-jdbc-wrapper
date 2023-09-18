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

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.dtsx.astra.sdk.db.AstraDbClient;
import com.dtsx.astra.sdk.db.domain.DatabaseStatusType;
import com.dtsx.astra.sdk.utils.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Test JDBC Driver against Astra.
 * To run this test an environment variable ASTRA_DB_APPLICATION_TOKEN must be set.
 * (Vector is fully deployed there)
 */
@TestMethodOrder(org.junit.jupiter.api.MethodOrderer.OrderAnnotation.class)
class DbaasAstraIntegrationTest {

    private static final String DATABASE_NAME = "test_cassandra_jdbc";
    private static final String KEYSPACE_NAME = "test";

    static CassandraConnection sqlConnection = null;

    @BeforeAll
    @EnabledIfEnvironmentVariable(named = "ASTRA_DB_APPLICATION_TOKEN", matches = "Astra.*")
    static void setupAstra() throws Exception {

        /*
         * Devops API Client, handy for tests
         */
        AstraDbClient astraDbClient = new AstraDbClient(TestUtils.getAstraToken());

        /*
         * Give a Database (create if not exist, resume if needed)
         * Vector Database is Cassandra DB with vector support enabled.
         *
         * TestUtils.setupDatabase(DATABASE_NAME, KEYSPACE_NAME);
         * It can take up to 1 min to create the database if not exists
         */
        String dbId = TestUtils.setupVectorDatabase(DATABASE_NAME, KEYSPACE_NAME);
        Assertions.assertTrue(astraDbClient.findById(dbId).isPresent());
        Assertions.assertEquals(DatabaseStatusType.ACTIVE, astraDbClient.findById(dbId).get().getStatus());

        /*
         * We need the cloud secure bundle to connect to the database.
         * - TestUtils will download it for you and saved in /tmp
         * - Single region so we can use the default
         */
        astraDbClient
            .database(dbId)
            .downloadDefaultSecureConnectBundle("/tmp/" + DATABASE_NAME + "_scb.zip");

        /*
         * Building jdbcUrl and sqlConnection.
         * Astra can be access with only a token (username='token')
         */
        sqlConnection = (CassandraConnection) DriverManager.getConnection(
            "jdbc:cassandra://dbaas/" + KEYSPACE_NAME +
            "?user=" + "token" +
            "&password=" + TestUtils.getAstraToken() + // env var ASTRA_DB_APPLICATION_TOKEN
            "&consistency=" + "LOCAL_QUORUM" +
            "&secureconnectbundle=/tmp/" + DATABASE_NAME + "_scb.zip");
    }
/*
    @Test
    @Order(1)
    @EnabledIfEnvironmentVariable(named = "ASTRA_DB_APPLICATION_TOKEN", matches = "Astra.*")
    void givenConnection_whenCreateTable_shouldTableExist() throws SQLException {
        // Given
        Assertions.assertNotNull(sqlConnection);
        // When
        sqlConnection.createStatement().execute(SchemaBuilder
            .createTable("simple_table")
            .ifNotExists()
            .withPartitionKey("email", DataTypes.TEXT)
            .withColumn("firstname", DataTypes.TEXT)
            .withColumn("lastname", DataTypes.TEXT)
            .build().getQuery());
        // Then
        Assertions.assertTrue(tableExist("simple_table"));
    }

    @Test
    @Order(2)
    @EnabledIfEnvironmentVariable(named = "ASTRA_DB_APPLICATION_TOKEN", matches = "Astra.*")
    void givenTable_whenInsert_shouldRetrieveData() throws Exception {
        // Given
        Assertions.assertTrue(tableExist("simple_table"));
        // When
        String insertSimpleCQL = "INSERT INTO simple_table (email, firstname, lastname) VALUES(?,?,?)";
        final CassandraPreparedStatement prepStatement = sqlConnection.prepareStatement(insertSimpleCQL);
        prepStatement.setString(1, "pierre.feuille@foo.com");
        prepStatement.setString(2, "pierre");
        prepStatement.setString(2, "feuille");
        prepStatement.execute();
        // Then (warning on Cassandra expected)
        Assertions.assertEquals(1, countRecords("simple_table"));
    }

    @Test
    @Order(3)
    @EnabledIfEnvironmentVariable(named = "ASTRA_DB_APPLICATION_TOKEN", matches = "Astra.*")
    void givenConnection_whenCreateTableVector_shouldTableExist() throws Exception {
        // When
        sqlConnection.createStatement().execute("" +
                    "CREATE TABLE IF NOT EXISTS pet_supply_vectors (" +
                    "    product_id     TEXT PRIMARY KEY," +
                    "    product_name   TEXT," +
                    "    product_vector vector<float, 14>)");
        // Then
        Assertions.assertTrue(tableExist("pet_supply_vectors"));
        sqlConnection.createStatement().execute("" +
                    "CREATE CUSTOM INDEX IF NOT EXISTS idx_vector " +
                    "ON pet_supply_vectors(product_vector) " +
                    "USING 'StorageAttachedIndex'");
        // When
        sqlConnection.createStatement().execute("" +
                    "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                    "VALUES ('pf1843','HealthyFresh - Chicken raw dog food',[1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0])");
        sqlConnection.createStatement().execute("" +
                    "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                    "VALUES ('pf1844','HealthyFresh - Beef raw dog food',[1, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0])");
        sqlConnection.createStatement().execute("" +
                    "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                    "VALUES ('pt0021','Dog Tennis Ball Toy',[0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0])");
        sqlConnection.createStatement().execute("" +
                    "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                    "VALUES ('pt0041','Dog Ring Chew Toy',[0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0])");
        sqlConnection.createStatement().execute("" +
                    "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                    "VALUES ('pf7043','PupperSausage Bacon dog Treats',[0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1])");
        sqlConnection.createStatement().execute("" +
                    "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                    "VALUES ('pf7044','PupperSausage Beef dog Treats',[0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 1, 0])");
        // Then (warning on Cassandra expected)
        Assertions.assertEquals(6, countRecords("pet_supply_vectors"));
    }*/

    @Test
    @Order(4)
    @EnabledIfEnvironmentVariable(named = "ASTRA_DB_APPLICATION_TOKEN", matches = "Astra.*")
    void givenVectorTable_whenSimilaritySearch_shouldReturnResults() throws Exception {
        // Given
        Assertions.assertTrue(tableExist("pet_supply_vectors"));
        Assertions.assertEquals(6, countRecords("pet_supply_vectors"));
        // When
        final CassandraPreparedStatement prepStatement = sqlConnection.prepareStatement("" +
            "SELECT\n" +
            "     product_id, product_vector,\n" +
            "     similarity_dot_product(product_vector,[1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]) as similarity\n" +
            "FROM pet_supply_vectors\n" +
            "ORDER BY product_vector\n" +
            "ANN OF [1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]\n" +
            "LIMIT 2;");
        java.sql.ResultSet rs = prepStatement.executeQuery();
        // A result has been found
        Assertions.assertTrue(rs.next());
        // Parsing Results
        Assertions.assertNotNull(rs.getObject("product_vector"));
        Assertions.assertEquals(3.0d, rs.getDouble("similarity"));
    }

    private boolean tableExist(String tableName) throws SQLException {
        String existTableCql = "select table_name,keyspace_name from system_schema.tables where keyspace_name=? and table_name=?";
        final CassandraPreparedStatement prepStatement = sqlConnection.prepareStatement(existTableCql);
        prepStatement.setString(1, KEYSPACE_NAME);
        prepStatement.setString(2, tableName);
        return prepStatement.executeQuery().next();
    }

    private int countRecords(String tablename) throws SQLException {
        String countRecordsCql = "select count(*) from " + tablename;
        final CassandraPreparedStatement prepStatement = sqlConnection.prepareStatement(countRecordsCql);
        final ResultSet resultSet = prepStatement.executeQuery();
        resultSet.next();
        return resultSet.getInt(1);
    }

    @AfterAll
    static void closeSql() throws SQLException {
        if (sqlConnection != null) {
            sqlConnection.close();
        }
    }

}


