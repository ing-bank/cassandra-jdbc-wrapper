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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Extension of {@link ResultSet} interface providing additional methods specific to Cassandra result sets for
 * JSON-formatted results provided by Cassandra features {@code SELECT JSON} and {@code toJson()}.
 * <p>
 *     The CQL {@code SELECT} queries using JSON support will return a map where the keys are the column labels. So,
 *     don't forget that if you use JSON formatting for only one column of type {@code T}, the result will be an object
 *     like this one: <code>{ "columnLabel": { ... } }</code> and should be deserialized to an object extending
 *     {@code Map<String, T>} and not directly to {@code T}.
 * </p>
 * Using the methods defined into this interface requires some considerations relative to the acceptable types for
 * result deserialization. The table below lists the acceptable type(s) in the target object to which the JSON will be
 * deserialized. Note that all non-collection types can also be deserialized to {@link String} and the collections can
 * be deserialized to {@link List} (for CQL types {@code list}, {@code set} and {@code tuple}) or {@link Map} (for CQL
 * types {@code map} and {@code udt}) of {@link String} items.
 *     <table border="1">
 *         <tr><th>CQL Type </th><th>JSON type  </th><th> Acceptable Java types         </th>
 *         <tr><td>ascii    </td><td>string     </td><td>{@link String}                 </td></tr>
 *         <tr><td>bigint   </td><td>integer    </td><td>{@link Number}                 </td></tr>
 *         <tr><td>blob     </td><td>string     </td><td>{@link ByteBuffer}             </td></tr>
 *         <tr><td>boolean  </td><td>boolean    </td><td>{@link Boolean}                </td></tr>
 *         <tr><td>date     </td><td>string     </td><td>{@link Date}, {@link LocalDate}</td></tr>
 *         <tr><td>decimal  </td><td>float      </td><td>{@link Number}                 </td></tr>
 *         <tr><td>double   </td><td>float      </td><td>{@link Number}                 </td></tr>
 *         <tr><td>float    </td><td>float      </td><td>{@link Number}                 </td></tr>
 *         <tr><td>inet     </td><td>string     </td><td>{@link InetAddress}            </td></tr>
 *         <tr><td>int      </td><td>integer    </td><td>{@link Number}                 </td></tr>
 *         <tr><td>list     </td><td>list       </td><td>{@link List}                   </td></tr>
 *         <tr><td>map      </td><td>map        </td><td>{@link Map}                    </td></tr>
 *         <tr><td>set      </td><td>list       </td><td>{@link Set}, {@link List}      </td></tr>
 *         <tr><td>smallint </td><td>integer    </td><td>{@link Number}                 </td></tr>
 *         <tr><td>text     </td><td>string     </td><td>{@link String}                 </td></tr>
 *         <tr><td>time     </td><td>string     </td><td>{@link LocalTime}              </td></tr>
 *         <tr><td>timestamp</td><td>string     </td><td>{@link OffsetDateTime}         </td></tr>
 *         <tr><td>timeuuid </td><td>string     </td><td>{@link UUID}                   </td></tr>
 *         <tr><td>tinyint  </td><td>integer    </td><td>{@link Number}                 </td></tr>
 *         <tr><td>tuple    </td><td>list       </td><td>{@link List}                   </td></tr>
 *         <tr><td>udt      </td><td>map        </td><td>{@link Map} or a suitable object to which the map can be
 *         deserialized                                                                 </td></tr>
 *         <tr><td>uuid     </td><td>string     </td><td>{@link UUID}                   </td></tr>
 *         <tr><td>varchar  </td><td>string     </td><td>{@link String}                 </td></tr>
 *         <tr><td>varint   </td><td>integer    </td><td>{@link Number}                 </td></tr>
 *     </table>
 *     See: <a href="https://cassandra.apache.org/doc/latest/cassandra/cql/json.html">
 *         CQL reference for JSON support</a>.
 */
public interface CassandraResultSetJsonSupport extends ResultSet {

    /**
     * Retrieves the value of the designated column in the current row of this {@code ResultSet} object as an instance
     * of type {@code T} from a JSON string.
     * <p>
     * Typically, this method can be called on a result set returned by a CQL query like:<br>
     * <code>
     * SELECT JSON col1, col2 FROM exampleTable;
     * </code>
     * <br>or on a result column defined with the function {@code toJson()}.
     * </p>
     *
     * @param columnIndex The column index (the first column is 1).
     * @param type        The class representing the Java data type to convert the designated column to.
     * @param <T>         The type of the object represented by the JSON string.
     * @return The column value converted to an instance of {@code T}. If the value is SQL {@code NULL}, it should
     * return {@code null}.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs; if this method is called
     *                      on a closed result set or if the value is cannot be parsed to an object of type {@code T}.
     * @see <a href="https://cassandra.apache.org/doc/latest/cassandra/cql/json.html">Cassandra JSON support</a>
     */
    <T> T getObjectFromJson(int columnIndex, Class<T> type) throws SQLException;

    /**
     * Retrieves the value of the designated column in the current row of this {@code ResultSet} object as an instance
     * of type {@code T} from a JSON string.
     * <p>
     * Typically, this method can be called on a result set returned by a CQL query like:<br>
     * <code>
     * SELECT JSON col1, col2 FROM exampleTable;
     * </code>
     * <br>or on a result column defined with the function {@code toJson()}.
     * </p>
     *
     * @param columnLabel The label for the column specified with the SQL AS clause. If the SQL AS clause was not
     *                    specified, then the label is the name of the column.
     * @param type        The class representing the Java data type to convert the designated column to.
     * @param <T>         The type of the object represented by the JSON string.
     * @return The column value converted to an instance of {@code T}. If the value is SQL {@code NULL}, it should
     * return {@code null}.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs; if this method is called
     *                      on a closed result set or if the value is cannot be parsed to an object of type {@code T}.
     * @see <a href="https://cassandra.apache.org/doc/latest/cassandra/cql/json.html">Cassandra JSON support</a>
     */
    <T> T getObjectFromJson(String columnLabel, Class<T> type) throws SQLException;

    /**
     * Retrieves the value of the column labelled "{@code [json]}", containing a JSON string, in the current row of
     * this {@code ResultSet} object as an instance of type {@code T}.
     * <p>
     * Typically, this method can be called on a result set returned by a CQL query like:<br>
     * <code>
     * SELECT JSON * FROM exampleTable;
     * </code>
     * </p>
     *
     * @param type The class representing the Java data type to convert the column value to.
     * @param <T>  The type of the object represented by the JSON string.
     * @return The column value converted to an instance of {@code T}. If the value is SQL {@code NULL}, it should
     * return {@code null}.
     * @throws SQLException if the columnIndex is not valid; if a database access error occurs; if this method is called
     *                      on a closed result set or if the value is cannot be parsed to an object of type {@code T}.
     * @see <a href="https://cassandra.apache.org/doc/latest/cassandra/cql/json.html">Cassandra JSON support</a>
     */
    <T> T getObjectFromJson(Class<T> type) throws SQLException;

}
