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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test CQL Vector data type
 */
class VectorsDseContainerTest extends UsingDseContainerTest {

    private static final String KEYSPACE = "test_keyspace_vect";

    @BeforeAll
    static void setup() throws Exception {
        initializeContainer("7.0.0-a");
        initConnection(KEYSPACE, "version=3.0.0", "localdatacenter=datacenter1");
    }

    @Test
    void givenVectorTable_whenSimilaritySearch_shouldReturnResults() throws Exception {
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

}
