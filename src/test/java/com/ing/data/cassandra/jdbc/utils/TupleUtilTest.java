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
package com.ing.data.cassandra.jdbc.utils;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.internal.core.data.DefaultTupleValue;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ing.data.cassandra.jdbc.utils.TupleUtil.tupleValueUsingFormattedContents;
import static com.ing.data.cassandra.jdbc.utils.TupleUtil.tupleValuesUsingFormattedContents;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TupleUtilTest {

    private static final TupleType testTupleType = new DefaultTupleType(Arrays.asList(DataTypes.INT, DataTypes.TEXT));
    private static final TupleValue testTupleValue = new DefaultTupleValue(testTupleType, 10, "abc");
    private static final TupleValue testTupleValue2 = new DefaultTupleValue(testTupleType, 20, "def");

    @Test
    void givenNullValue_whenGetTupleValueUsingFormattedContents_returnNull() {
        assertNull(tupleValueUsingFormattedContents(null));
    }

    @Test
    void givenTupleValue_whenGetTupleValueUsingFormattedContents_returnTupleValueUsingFormattedContents() {
        assertEquals("(10,'abc')", tupleValueUsingFormattedContents(testTupleValue).toString());
    }

    @Test
    void givenTupleValueWithNullFields_whenGetTupleValueUsingFormattedContents_returnTupleValueUsingFormattedContents()
    {
        assertEquals("(10,NULL)",
            tupleValueUsingFormattedContents(new DefaultTupleValue(testTupleType, 10)).toString());
    }

    @Test
    void givenNullList_whenGetTupleValueUsingFormattedContents_returnNull() {
        assertNull(tupleValuesUsingFormattedContents((List<UdtValue>) null));
    }

    @Test
    void givenListOfTupleValues_whenGetTupleValuesUsingFormattedContents_returnTupleValuesListUsingFormattedContents() {
        assertEquals("[(10,'abc'), (20,'def')]",
            tupleValuesUsingFormattedContents(Arrays.asList(testTupleValue, testTupleValue2)).toString());
    }

    @Test
    void givenNullSet_whenGetTupleValuesUsingFormattedContents_returnNull() {
        assertNull(tupleValuesUsingFormattedContents((Set<UdtValue>) null));
    }

    @Test
    void givenSetOfTupleValues_whenGetTupleValuesUsingFormattedContents_returnTupleValuesSetUsingFormattedContents() {
        final Set<TupleValue> testSet = new LinkedHashSet<>();
        testSet.add(testTupleValue);
        testSet.add(testTupleValue2);
        assertEquals("[(10,'abc'), (20,'def')]", tupleValuesUsingFormattedContents(testSet).toString());
    }

    @Test
    void givenNullMap_whenGetTupleValuesUsingFormattedContents_returnNull() {
        assertNull(tupleValuesUsingFormattedContents((Map<String, UdtValue>) null));
    }

    @Test
    void givenMapWithTupleValues_whenGetTupleValuesUsingFormattedContents_returnTupleValuesMapUsingFormattedContents() {
        final Map<String, TupleValue> testMap = new LinkedHashMap<>();
        testMap.put("mapKey1", testTupleValue);
        testMap.put("mapKey2", testTupleValue2);
        assertEquals("{mapKey1=(10,'abc'), mapKey2=(20,'def')}",
            tupleValuesUsingFormattedContents(testMap).toString());

        final Map<TupleValue, Integer> testMap2 = new LinkedHashMap<>();
        testMap2.put(testTupleValue, 1);
        testMap2.put(testTupleValue2, 2);
        assertEquals("{(10,'abc')=1, (20,'def')=2}",
            tupleValuesUsingFormattedContents(testMap2).toString());
    }

}
