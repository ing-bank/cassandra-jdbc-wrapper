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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.sql.ResultSet;

import org.junit.jupiter.api.Test;

/**
 * Test Empty Cassandra Result Sets
 */
class EmptyResultSetUnitTest {

    @Test
    void givenEmptyResult_whenClose_dontThrow() throws Exception {
        final ResultSet rs = CassandraResultSet.EMPTY_RESULT_SET;

        assertFalse(rs.isClosed(), "Empty ResultSet should be open in the beginnning");
        assertFalse(rs.next(), "Empty ResultSet should have no rows");

        assertDoesNotThrow(() -> rs.close(), "Do not throw an exception, but still close the ResultSet");
        assertTrue(rs.isClosed(), "ResultSet should be closed after calling close()");
    }
}
