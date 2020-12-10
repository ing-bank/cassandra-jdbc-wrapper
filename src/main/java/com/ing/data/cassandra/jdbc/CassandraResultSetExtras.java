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

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CassandraResultSetExtras extends ResultSet {
    /**
     * @return the current row key
     * @throws SQLException
     */
    byte[] getKey() throws SQLException;

    BigInteger getBigInteger(int i) throws SQLException;

    BigInteger getBigInteger(String name) throws SQLException;

    List<?> getList(int index) throws SQLException;

    List<?> getList(String name) throws SQLException;

    Set<?> getSet(int index) throws SQLException;

    Set<?> getSet(String name) throws SQLException;

    Map<?, ?> getMap(int index) throws SQLException;

    Map<?, ?> getMap(String name) throws SQLException;

}
