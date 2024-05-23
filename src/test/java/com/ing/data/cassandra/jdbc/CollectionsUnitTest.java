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

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test CQL Collections Data Types
 * List
 * Map
 * Set
 */
class CollectionsUnitTest extends UsingCassandraContainerTest {

    private static final String KEYSPACE = "test_keyspace_coll";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
    }

    @Test
    void givenListInsertStatement_whenExecute_insertExpectedValues() throws Exception {
        final Statement statement = sqlConnection.createStatement();

        final String insert = "INSERT INTO collections_test (keyValue, listValue) VALUES(2, [4, 6, 789]);";
        statement.executeUpdate(insert);

        final ResultSet resultSet = statement.executeQuery("SELECT * FROM collections_test WHERE keyValue = 2;");
        resultSet.next();

        assertThat(resultSet, is(instanceOf(CassandraResultSet.class)));
        assertEquals(2, resultSet.getInt("keyValue"));

        final List<?> listObject = ((CassandraResultSet) resultSet).getList("listValue");
        assertThat(listObject, is(instanceOf(ArrayList.class)));
        assertEquals(3, listObject.size());
        assertEquals(789L, listObject.get(2));

        statement.close();
    }

    @Test
    void givenListUpdateStatement_whenExecute_updateExpectedValues() throws Exception {
        final Statement statement = sqlConnection.createStatement();

        // Add items to the list.
        final String updateQuery1 = "UPDATE collections_test SET listValue = listValue + [2, 4, 6] WHERE keyValue = 1;";
        statement.executeUpdate(updateQuery1);

        ResultSet resultSet = statement.executeQuery("SELECT * FROM collections_test WHERE keyValue = 1;");
        resultSet.next();

        assertEquals(1, resultSet.getInt("keyValue"));
        List<?> listObject = ((CassandraResultSet) resultSet).getList("listValue");
        assertThat(listObject, is(instanceOf(ArrayList.class)));
        assertEquals(6, listObject.size());
        assertEquals(12345L, listObject.get(2));
        assertEquals(6L, listObject.get(5));

        // Replace the existing list.
        final String updateQuery2 = "UPDATE collections_test SET listValue = [98, 99, 100] WHERE keyValue = 1;";
        statement.executeUpdate(updateQuery2);

        resultSet = statement.executeQuery("SELECT * FROM collections_test WHERE keyValue = 1;");
        resultSet.next();

        listObject = ((CassandraResultSet) resultSet).getList("listValue");
        assertEquals(3, listObject.size());
        assertEquals(98L, listObject.get(0));
        assertEquals(100L, listObject.get(2));

        // Replace one item of the list.
        final String updateQuery3 = "UPDATE collections_test SET listValue[0] = 2000 WHERE keyValue = 1;";
        statement.executeUpdate(updateQuery3);

        resultSet = statement.executeQuery("SELECT * FROM collections_test WHERE keyValue = 1;");
        resultSet.next();

        listObject = ((CassandraResultSet) resultSet).getList("listValue");
        assertEquals(3, listObject.size());
        assertEquals(2000L, listObject.get(0));

        // Replace the existing list using a prepared statement.
        final String updateQuery4 = "UPDATE collections_test SET listValue = ? WHERE keyValue = 1;";
        final PreparedStatement preparedStatement = sqlConnection.prepareStatement(updateQuery4);
        final List<Long> newList = new ArrayList<>();
        newList.add(888L);
        newList.add(999L);
        preparedStatement.setObject(1, newList, Types.OTHER);
        preparedStatement.execute();

        resultSet = statement.executeQuery("SELECT * FROM collections_test WHERE keyValue = 1;");
        resultSet.next();

        listObject = ((CassandraResultSet) resultSet).getList("listValue");
        assertEquals(2, listObject.size());
        assertEquals(888L, listObject.get(0));
        assertEquals(999L, listObject.get(1));
    }

    @Test
    void givenSetInsertStatement_whenExecute_insertExpectedValues() throws Exception {
        final Statement statement = sqlConnection.createStatement();

        final String insert = "INSERT INTO collections_test (keyValue, setValue) VALUES(2, {'yellow', 'green'});";
        statement.executeUpdate(insert);

        final ResultSet resultSet = statement.executeQuery("SELECT * FROM collections_test WHERE keyValue = 2;");
        resultSet.next();

        assertThat(resultSet, is(instanceOf(CassandraResultSet.class)));
        assertEquals(2, resultSet.getInt("keyValue"));

        final Set<?> setObject = ((CassandraResultSet) resultSet).getSet("setValue");
        assertThat(setObject, is(instanceOf(LinkedHashSet.class)));
        assertEquals(2, setObject.size());
        assertTrue(setObject.contains("yellow"));
        assertTrue(setObject.contains("green"));

        statement.close();
    }

    @Test
    void givenSetUpdateStatement_whenExecute_updateExpectedValues() throws Exception {
        final Statement statement = sqlConnection.createStatement();

        // Add items to the set.
        final String updateQuery1 = "UPDATE collections_test SET setValue = setValue + {'black', 'orange'} " +
            "WHERE keyValue = 1;";
        statement.executeUpdate(updateQuery1);

        ResultSet resultSet = statement.executeQuery("SELECT * FROM collections_test WHERE keyValue = 1;");
        resultSet.next();

        assertEquals(1, resultSet.getInt("keyValue"));
        Set<?> setObject = ((CassandraResultSet) resultSet).getSet("setValue");
        assertThat(setObject, is(instanceOf(LinkedHashSet.class)));
        assertEquals(5, setObject.size());
        assertTrue(setObject.contains("black"));
        assertTrue(setObject.contains("orange"));
        assertTrue(setObject.contains("white"));

        // Add items to the set.
        final String updateQuery2 = "UPDATE collections_test SET setValue = setValue - {'red'} WHERE keyValue = 1;";
        statement.executeUpdate(updateQuery2);

        resultSet = statement.executeQuery("SELECT * FROM collections_test WHERE keyValue = 1;");
        resultSet.next();

        setObject = ((CassandraResultSet) resultSet).getSet("setValue");
        assertEquals(4, setObject.size());
        assertTrue(setObject.contains("black"));
        assertTrue(setObject.contains("white"));
        assertTrue(setObject.contains("blue"));
        assertFalse(setObject.contains("red"));

        // Replace the existing set using a prepared statement.
        final String updateQuery3 = "UPDATE collections_test SET setValue = ? WHERE keyValue = 1;";
        final PreparedStatement preparedStatement = sqlConnection.prepareStatement(updateQuery3);
        final Set<String> newSet = new HashSet<>();
        newSet.add("pink");
        newSet.add("gray");
        preparedStatement.setObject(1, newSet, Types.OTHER);
        preparedStatement.execute();

        resultSet = statement.executeQuery("SELECT * FROM collections_test WHERE keyValue = 1;");
        resultSet.next();

        setObject = ((CassandraResultSet) resultSet).getSet("setValue");
        assertEquals(2, setObject.size());
        assertTrue(setObject.contains("pink"));
        assertTrue(setObject.contains("gray"));
        assertFalse(setObject.contains("blue"));
        assertFalse(setObject.contains("black"));
    }

    @Test
    void givenMapInsertStatement_whenExecute_insertExpectedValues() throws Exception {
        final Statement statement = sqlConnection.createStatement();

        final String insert = "INSERT INTO collections_test (keyValue, mapValue) " +
            "VALUES(2, {1.0: true, 3.0: false, 5.0 : false});";
        statement.executeUpdate(insert);

        final ResultSet resultSet = statement.executeQuery("SELECT * FROM collections_test WHERE keyValue = 2;");
        resultSet.next();

        assertThat(resultSet, is(instanceOf(CassandraResultSet.class)));
        assertEquals(2, resultSet.getInt("keyValue"));

        final Map<?, ?> mapObject = ((CassandraResultSet) resultSet).getMap("mapValue");
        assertThat(mapObject, is(instanceOf(HashMap.class)));
        assertEquals(3, mapObject.size());
        assertTrue(mapObject.containsKey(1.0));
        assertTrue(mapObject.containsKey(5.0));
        assertEquals(true, mapObject.get(1.0));
        assertEquals(false, mapObject.get(5.0));

        statement.close();
    }

    @Test
    void givenMapUpdateStatement_whenExecute_updateExpectedValues() throws Exception {
        final Statement statement = sqlConnection.createStatement();

        // Add items to the map.
        final String updateQuery1 = "UPDATE collections_test " +
            "SET mapValue = mapValue + {1.0: true, 3.0: false, 5.0: false} WHERE keyValue = 1;";
        statement.executeUpdate(updateQuery1);

        ResultSet resultSet = statement.executeQuery("SELECT * FROM collections_test WHERE keyValue = 1;");
        resultSet.next();

        assertEquals(1, resultSet.getInt("keyValue"));
        Map<?, ?> mapObject = ((CassandraResultSet) resultSet).getMap("mapValue");
        assertThat(mapObject, is(instanceOf(HashMap.class)));
        assertEquals(6, mapObject.size());
        assertTrue(mapObject.containsKey(3.0));
        assertTrue(mapObject.containsKey(6.0));
        assertEquals(false, mapObject.get(3.0));
        assertEquals(true, mapObject.get(6.0));

        // Remove an item from the map.
        final String updateQuery2 = "DELETE mapValue[6.0] FROM collections_test WHERE keyValue = 1;";
        statement.executeUpdate(updateQuery2);

        resultSet = statement.executeQuery("SELECT * FROM collections_test WHERE keyValue = 1;");
        resultSet.next();

        mapObject = ((CassandraResultSet) resultSet).getMap("mapValue");
        assertEquals(5, mapObject.size());
        assertTrue(mapObject.containsKey(5.0));
        assertFalse(mapObject.containsKey(6.0));
        assertEquals(false, mapObject.get(5.0));

        // Replace the existing map using a prepared statement.
        final String updateQuery3 = "UPDATE collections_test SET mapValue = ? WHERE keyValue = 1;";
        final PreparedStatement preparedStatement = sqlConnection.prepareStatement(updateQuery3);
        final Map<Double, Boolean> newMap = new LinkedHashMap<>();
        newMap.put(10.0, false);
        newMap.put(12.0, true);
        preparedStatement.setObject(1, newMap, Types.OTHER);
        preparedStatement.execute();

        resultSet = statement.executeQuery("SELECT * FROM collections_test WHERE keyValue = 1;");
        resultSet.next();

        mapObject = ((CassandraResultSet) resultSet).getMap("mapValue");
        assertEquals(2, mapObject.size());
        assertTrue(mapObject.containsKey(10.0));
        assertTrue(mapObject.containsKey(12.0));
        assertFalse(mapObject.containsKey(2.0));
        assertEquals(false, mapObject.get(10.0));
        assertEquals(true, mapObject.get(12.0));
    }

    @Test
    void givenFrozenTypesSelectStatement_whenExecute_getExpectedResultSet() throws Exception {
        final Statement statement = sqlConnection.createStatement();

        final ResultSet resultSet = statement.executeQuery("SELECT * FROM frozen_test WHERE keyValue = 1;");
        resultSet.next();

        assertThat(resultSet, is(instanceOf(CassandraResultSet.class)));
        assertEquals(1, resultSet.getInt("keyValue"));

        final List<?> listObject = ((CassandraResultSet) resultSet).getList("frozenList");
        assertThat(listObject, is(instanceOf(ArrayList.class)));
        assertEquals(3, listObject.size());
        assertEquals(1, listObject.get(0));
        assertEquals(3, listObject.get(1));
        assertEquals(123, listObject.get(2));

        final Map<?, ?> mapObject = ((CassandraResultSet) resultSet).getMap("frozenMap");
        assertThat(mapObject, is(instanceOf(HashMap.class)));
        assertEquals(2, mapObject.size());
        assertTrue(mapObject.containsKey("k1"));
        assertTrue(mapObject.containsKey("k2"));
        assertEquals("v1", mapObject.get("k1"));
        assertEquals("v2", mapObject.get("k2"));

        final Set<?> setObject = ((CassandraResultSet) resultSet).getSet("frozenSet");
        assertThat(setObject, is(instanceOf(LinkedHashSet.class)));
        assertEquals(3, setObject.size());
        assertTrue(setObject.contains("i1"));
        assertTrue(setObject.contains("i2"));
        assertTrue(setObject.contains("i3"));

        final List<?> tuplesListObject = ((CassandraResultSet) resultSet).getList("innerTuple");
        assertThat(tuplesListObject, is(instanceOf(ArrayList.class)));
        assertEquals(2, tuplesListObject.size());
        assertThat(tuplesListObject.get(0), is(instanceOf(TupleValue.class)));
        final TupleValue tupleValue = (TupleValue) tuplesListObject.get(0);
        assertEquals(1, tupleValue.getInt(0));
        assertEquals("val1", tupleValue.getString(1));
        final UdtValue udtValueInTuple = tupleValue.getUdtValue(2);
        assertNotNull(udtValueInTuple);
        assertEquals(10, udtValueInTuple.getInt("key"));
        assertEquals("test", udtValueInTuple.getString("value1"));
        assertTrue(udtValueInTuple.getBoolean("value2"));

        final List<?> udtListObject = ((CassandraResultSet) resultSet).getList("innerUdt");
        assertThat(udtListObject, is(instanceOf(ArrayList.class)));
        assertEquals(2, udtListObject.size());
        assertThat(udtListObject.get(0), is(instanceOf(UdtValue.class)));
        final UdtValue udtValue = (UdtValue) udtListObject.get(0);
        assertEquals(1, udtValue.getInt("key"));
        assertEquals("test", udtValue.getString("value1"));
        assertTrue(udtValue.getBoolean("value2"));

        Object outerUdt = resultSet.getObject("outerUdt");
        assertInstanceOf(UdtValue.class, outerUdt);
        assertEquals(1, ((UdtValue) outerUdt).getInt("key"));

        statement.close();
    }

}
