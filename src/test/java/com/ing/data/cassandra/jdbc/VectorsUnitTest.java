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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
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
// FIXME: Implement vector testing when Cassandra 5.0 is available.
@Disabled
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
        assertEquals(2.1, floatsVector.get(0));
        assertEquals(3.7, floatsVector.get(1));
        assertEquals(9.0, floatsVector.get(2));
        assertEquals(5.5, floatsVector.get(2));

        statement.close();
    }

}
