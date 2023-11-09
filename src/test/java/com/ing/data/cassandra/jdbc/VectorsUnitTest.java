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

import com.datastax.oss.driver.api.core.data.CqlVector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test CQL Vector data type
 */
class VectorsUnitTest extends UsingCassandraContainerTest {

    private static final String KEYSPACE = "test_keyspace_vect";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
    }

    @Test
    void givenVectorInsertStatement_whenExecute_insertExpectedValues() throws Exception {
        final Statement statement = sqlConnection.createStatement();

        final String insert = "INSERT INTO vectors_test (keyValue, intsVector, floatsVector) "
            + "VALUES(1, [4, 6, 8], [2.1, 3.7, 9.0, 5.5]);";
        statement.executeUpdate(insert);

        final ResultSet resultSet = statement.executeQuery("SELECT * FROM vectors_test WHERE keyValue = 1;");
        resultSet.next();

        assertThat(resultSet, is(instanceOf(CassandraResultSet.class)));
        assertEquals(1, resultSet.getInt("keyValue"));

        final CqlVector<?> intsVector = ((CassandraResultSet) resultSet).getVector("intsVector");
        assertEquals(3, intsVector.size());
        assertEquals(4, intsVector.get(0));
        assertEquals(6, intsVector.get(1));
        assertEquals(8, intsVector.get(2));
        final CqlVector<?> floatsVector = ((CassandraResultSet) resultSet).getVector(2);
        assertEquals(4, floatsVector.size());
        assertEquals(2.1f, floatsVector.get(0));
        assertEquals(3.7f, floatsVector.get(1));
        assertEquals(9.0f, floatsVector.get(2));
        assertEquals(5.5f, floatsVector.get(3));

        statement.close();
    }

    @Test
    void givenVectorTable_whenSimilaritySearch_shouldReturnResults() throws Exception {
        final CassandraPreparedStatement prepStatement = sqlConnection.prepareStatement(
            "SELECT product_id, product_vector,"
                + "similarity_dot_product(product_vector,[1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]) as similarity "
                + "FROM pet_supply_vectors ORDER BY product_vector ANN OF [1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0] "
                + "LIMIT 2");
        java.sql.ResultSet rs = prepStatement.executeQuery();
        Assertions.assertTrue(rs.next());
        Assertions.assertNotNull(rs.getObject("product_vector"));
        Assertions.assertEquals(3.0d, rs.getDouble("similarity"));
    }

}
