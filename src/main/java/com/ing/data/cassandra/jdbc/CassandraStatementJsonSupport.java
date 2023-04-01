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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Extension of {@link PreparedStatement} interface providing additional methods specific to Cassandra statements using
 * JSON features {@code INSERT INTO ... JSON} and {@code fromJson()}.
 * <br>
 * Using the methods defined into this interface requires some considerations relative to the acceptable types for
 * serialization. The table below lists the acceptable type(s) in the Java object serialized to JSON to be correctly
 * parsed on Cassandra side. Note that all non-collection fields in the Java object to serialize can be {@link String}
 * (be careful with the format to use for some CQL types like dates for example) and the collections can contain
 * {@link String} items.
 *     <table border="1">
 *         <tr><th>CQL Type </th><th>Accepted JSON types   </th><th> Acceptable Java types                 </th>
 *         <tr><td>ascii    </td><td>string                </td><td>{@link String}                         </td></tr>
 *         <tr><td>bigint   </td><td>integer, string       </td><td>{@link Integer}, {@link BigInteger}    </td></tr>
 *         <tr><td>blob     </td><td>string                </td><td>{@link ByteBuffer}, {@link String} (should be an
 *         even-length hexadecimal representation of the blob bytes starting with '0x')                    </td></tr>
 *         <tr><td>boolean  </td><td>boolean, string       </td><td>{@link Boolean}                        </td></tr>
 *         <tr><td>date     </td><td>string                </td><td>{@link Date}, {@link LocalDate}        </td></tr>
 *         <tr><td>decimal  </td><td>integer, float, string</td><td>{@link Number}                         </td></tr>
 *         <tr><td>double   </td><td>integer, float, string</td><td>{@link Number}                         </td></tr>
 *         <tr><td>float    </td><td>integer, float, string</td><td>{@link Number}                         </td></tr>
 *         <tr><td>inet     </td><td>string                </td><td>{@link InetAddress}                    </td></tr>
 *         <tr><td>int      </td><td>integer, string       </td><td>{@link Integer}                        </td></tr>
 *         <tr><td>list     </td><td>list, string          </td><td>{@link List}                           </td></tr>
 *         <tr><td>map      </td><td>map, string           </td><td>{@link Map}                            </td></tr>
 *         <tr><td>set      </td><td>list, string          </td><td>{@link Set}, {@link List}              </td></tr>
 *         <tr><td>smallint </td><td>integer, string       </td><td>{@link Short}                          </td></tr>
 *         <tr><td>text     </td><td>string                </td><td>{@link String}                         </td></tr>
 *         <tr><td>time     </td><td>string                </td><td>{@link LocalTime}                      </td></tr>
 *         <tr><td>timestamp</td><td>integer, string       </td><td>{@link Integer}, {@link OffsetDateTime}</td></tr>
 *         <tr><td>timeuuid </td><td>string                </td><td>{@link UUID}                           </td></tr>
 *         <tr><td>tinyint  </td><td>integer, string       </td><td>{@link Byte}                           </td></tr>
 *         <tr><td>tuple    </td><td>list, string          </td><td>{@link List}                           </td></tr>
 *         <tr><td>udt      </td><td>map, string           </td><td>{@link Map} or a suitable object serializable
 *         and matching the target UDT                                                                     </td></tr>
 *         <tr><td>uuid     </td><td>string                </td><td>{@link UUID}                           </td></tr>
 *         <tr><td>varchar  </td><td>string                </td><td>{@link String}                         </td></tr>
 *         <tr><td>varint   </td><td>integer, string       </td><td>{@link Integer}, {@link BigInteger}    </td></tr>
 *     </table>
 *     See: <a href="https://cassandra.apache.org/doc/latest/cassandra/cql/json.html">
 *         CQL reference for JSON support</a>.
 */
public interface CassandraStatementJsonSupport extends PreparedStatement {

    /**
     * Sets the value of the designated parameter with the given object converted to a JSON string.
     * <p>
     * Typically, this method can be called to bind a JSON parameter in a CQL query like:<br>
     * <code>
     * INSERT INTO exampleTable JSON ?;
     * </code>
     * <br>or on a specific column using with the function {@code fromJson()}:<br>
     * <code>
     * INSERT INTO exampleTable (item) VALUES (fromJson(?));
     * </code>
     * </p>
     *
     * @param parameterIndex The parameter index (the first parameter is 1).
     * @param x              The object containing the input parameter value.
     * @param <T>            The type of the original object before conversion to a JSON string.
     * @throws SQLException if the parameter index does not correspond to a parameter marker in the SQL statement; if a
     *                      database access error occurs; if this method is called on a closed {@link PreparedStatement}
     *                      or if the object specified by {@code x} cannot be converted into a valid JSON string.
     */
    <T> void setJson(int parameterIndex, T x) throws SQLException;

}
