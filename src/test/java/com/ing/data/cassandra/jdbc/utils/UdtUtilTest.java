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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.data.DefaultUdtValue;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class UdtUtilTest {

    private static final UserDefinedType testUdt = new DefaultUserDefinedType(
        CqlIdentifier.fromCql("testkeyspace"),
        CqlIdentifier.fromCql("testudt"),
        false,
        Arrays.asList(CqlIdentifier.fromCql("key1"), CqlIdentifier.fromCql("key2")),
        Arrays.asList(DataTypes.INT, DataTypes.TEXT));
    private static final UdtValue testUdtValue = new DefaultUdtValue(testUdt, 10, "abc");
    private static final UdtValue testUdtValue2 = new DefaultUdtValue(testUdt, 20, "def");

    @Test
    void givenNullValue_whenGetUdtValueUsingFormattedContents_returnNull() {
        assertNull(UdtUtil.udtValueUsingFormattedContents(null));
    }

    @Test
    void givenUdtValue_whenGetUdtValueUsingFormattedContents_returnUdtValueUsingFormattedContents() {
        assertEquals("{key1:10,key2:'abc'}", UdtUtil.udtValueUsingFormattedContents(testUdtValue).toString());
    }

    @Test
    void givenUdtValueWithNullFields_whenGetUdtValueUsingFormattedContents_returnUdtValueUsingFormattedContents() {
        assertEquals("{key1:10,key2:NULL}",
            UdtUtil.udtValueUsingFormattedContents(new DefaultUdtValue(testUdt, 10)).toString());
    }

    @Test
    void givenNullList_whenGetUdtValueUsingFormattedContents_returnNull() {
        assertNull(UdtUtil.udtValuesUsingFormattedContents((List<UdtValue>) null));
    }

    @Test
    void givenListOfUdtValues_whenGetUdtValuesUsingFormattedContents_returnUdtValuesListUsingFormattedContents() {
        assertEquals("[{key1:10,key2:'abc'}, {key1:20,key2:'def'}]",
            UdtUtil.udtValuesUsingFormattedContents(Arrays.asList(testUdtValue, testUdtValue2)).toString());
    }

    @Test
    void givenNullSet_whenGetUdtValuesUsingFormattedContents_returnNull() {
        assertNull(UdtUtil.udtValuesUsingFormattedContents((Set<UdtValue>) null));
    }

    @Test
    void givenSetOfUdtValues_whenGetUdtValuesUsingFormattedContents_returnUdtValuesSetUsingFormattedContents() {
        final Set<UdtValue> testSet = new LinkedHashSet<>();
        testSet.add(testUdtValue);
        testSet.add(testUdtValue2);
        assertEquals("[{key1:10,key2:'abc'}, {key1:20,key2:'def'}]",
            UdtUtil.udtValuesUsingFormattedContents(testSet).toString());
    }

    @Test
    void givenNullMap_whenGetUdtValuesUsingFormattedContents_returnNull() {
        assertNull(UdtUtil.udtValuesUsingFormattedContents((Map<String, UdtValue>) null));
    }

    @Test
    void givenMapWithUdtValues_whenGetUdtValuesUsingFormattedContents_returnUdtValuesMapUsingFormattedContents() {
        final Map<String, UdtValue> testMap = new LinkedHashMap<>();
        testMap.put("mapKey1", testUdtValue);
        testMap.put("mapKey2", testUdtValue2);
        assertEquals("{mapKey1={key1:10,key2:'abc'}, mapKey2={key1:20,key2:'def'}}",
            UdtUtil.udtValuesUsingFormattedContents(testMap).toString());

        final Map<UdtValue, Integer> testMap2 = new LinkedHashMap<>();
        testMap2.put(testUdtValue, 1);
        testMap2.put(testUdtValue2, 2);
        assertEquals("{{key1:10,key2:'abc'}=1, {key1:20,key2:'def'}=2}",
            UdtUtil.udtValuesUsingFormattedContents(testMap2).toString());
    }

}
