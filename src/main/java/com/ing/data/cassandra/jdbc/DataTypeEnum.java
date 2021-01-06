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

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.protocol.internal.ProtocolConstants.DataType;
import com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.cassandra.db.marshal.CollectionType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Enumeration of CQL data types and the corresponding Java types.
 */
public enum DataTypeEnum {

    ASCII(DataType.ASCII, String.class, cqlName(DataTypes.ASCII)),
    BIGINT(DataType.BIGINT, Long.class, cqlName(DataTypes.BIGINT)),
    BLOB(DataType.BLOB, ByteBuffer.class, cqlName(DataTypes.BLOB)),
    BOOLEAN(DataType.BOOLEAN, Boolean.class, cqlName(DataTypes.BOOLEAN)),
    COUNTER(DataType.COUNTER, Long.class, cqlName(DataTypes.COUNTER)),
    DECIMAL(DataType.DECIMAL, BigDecimal.class, cqlName(DataTypes.DECIMAL)),
    DOUBLE(DataType.DOUBLE, Double.class, cqlName(DataTypes.DOUBLE)),
    FLOAT(DataType.FLOAT, Float.class, cqlName(DataTypes.FLOAT)),
    INET(DataType.INET, InetAddress.class, cqlName(DataTypes.INET)),
    INT(DataType.INT, Integer.class, cqlName(DataTypes.INT)),
    TEXT(DataType.VARCHAR, String.class, cqlName(DataTypes.TEXT)),
    TIMESTAMP(DataType.TIMESTAMP, Date.class, cqlName(DataTypes.TIMESTAMP)),
    UUID(DataType.UUID, UUID.class, cqlName(DataTypes.UUID)),
    VARCHAR(DataType.VARCHAR, String.class, "VARCHAR"),
    VARINT(DataType.VARINT, BigInteger.class, cqlName(DataTypes.VARINT)),
    TIMEUUID(DataType.TIMEUUID, UUID.class, cqlName(DataTypes.TIMEUUID)),
    LIST(DataType.LIST, List.class, CollectionType.Kind.LIST.name().toLowerCase()),
    SET(DataType.SET, Set.class, CollectionType.Kind.SET.name().toLowerCase()),
    MAP(DataType.MAP, Map.class, CollectionType.Kind.MAP.name().toLowerCase()),
    UDT(DataType.UDT, UdtValue.class, "UDT"),
    TUPLE(DataType.TUPLE, TupleValue.class, "TUPLE"),
    CUSTOM(DataType.CUSTOM, ByteBuffer.class, "CUSTOM"),
    SMALLINT(DataType.SMALLINT, Integer.class, cqlName(DataTypes.SMALLINT)),     // TODO: use Short.class
    TINYINT(DataType.TINYINT, Integer.class, cqlName(DataTypes.TINYINT)),        // TODO: use Byte.class
    DATE(DataType.DATE, Date.class, cqlName(DataTypes.DATE)),
    TIME(DataType.TIME, Date.class, cqlName(DataTypes.TIME)),                    // TODO: use Time.class
    DURATION(DataType.DURATION, Duration.class, cqlName(DataTypes.DURATION));

    final int protocolId;
    final Class<?> javaType;
    final String cqlType;

    private static final Map<String, DataTypeEnum> cqlDataTypeToDataType;
    static {
        cqlDataTypeToDataType = Maps.newHashMap();
        for (final DataTypeEnum dataType : DataTypeEnum.values()) {
            cqlDataTypeToDataType.put(dataType.cqlType, dataType);
        }
    }

    /**
     * Constructs a {@code DataTypeEnum} item.
     *
     * @param protocolId The type ID as defined in CQL binary protocol. (see
     *                   <a href="https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec">
     *                   CQL binary protocol definition</a> and {@link DataType}).
     * @param javaType   The corresponding Java type.
     * @param cqlType    The CQL type name.
     */
    DataTypeEnum(final int protocolId, final Class<?> javaType, final String cqlType) {
        this.protocolId = protocolId;
        this.javaType = javaType;
        this.cqlType = cqlType;
    }

    /**
     * Gets an enumeration item from a CQL type name.
     *
     * @param cqlTypeName The CQL type name.
     * @return The enumeration item corresponding to the given CQL type name.
     */
    static DataTypeEnum fromCqlTypeName(String cqlTypeName) {
        // Manage collection types (e.g. "list<varchar>")
        final int collectionTypeCharPos = cqlTypeName.indexOf("<");
        if (collectionTypeCharPos > 0) {
            cqlTypeName = cqlTypeName.substring(0, collectionTypeCharPos);
        }
        return cqlDataTypeToDataType.get(cqlTypeName);
    }

    /**
     * Returns whether this data type name represents the name of a collection type (i.e. that is a list, set or map).
     *
     * @return {@code true} if this data type name represents the name of a collection type, {@code false} otherwise.
     */
    public boolean isCollection() {
        switch (this) {
            case LIST:
            case SET:
            case MAP:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns the Java Class corresponding to this CQL type name.
     * <br/>
     * The correspondence between CQL types and Java ones is as follow:
     * <table border="1">
     * <caption>DataType to Java class correspondence</caption>
     * <tr><th>CQL DataType</th><th>Java class</th></tr>
     * <tr><td>ASCII         </td><td>{@link String}</td></tr>
     * <tr><td>BIGINT        </td><td>{@link Long}</td></tr>
     * <tr><td>BLOB          </td><td>{@link ByteBuffer}</td></tr>
     * <tr><td>BOOLEAN       </td><td>{@link Boolean}</td></tr>
     * <tr><td>COUNTER       </td><td>{@link Long}</td></tr>
     * <tr><td>CUSTOM        </td><td>{@link ByteBuffer}</td></tr>
     * <tr><td>DECIMAL       </td><td>{@link BigDecimal}</td></tr>
     * <tr><td>DOUBLE        </td><td>{@link Double}</td></tr>
     * <tr><td>DURATION      </td><td>{@link Duration}</td></tr>
     * <tr><td>FLOAT         </td><td>{@link Float}</td></tr>
     * <tr><td>INET          </td><td>{@link InetAddress}</td></tr>
     * <tr><td>INT           </td><td>{@link Integer}</td></tr>
     * <tr><td>LIST          </td><td>{@link List}</td></tr>
     * <tr><td>MAP           </td><td>{@link Map}</td></tr>
     * <tr><td>SET           </td><td>{@link Set}</td></tr>
     * <tr><td>SMALLINT      </td><td>{@link Integer}</td></tr>
     * <tr><td>TEXT          </td><td>{@link String}</td></tr>
     * <tr><td>TIME          </td><td>{@link Date}</td></tr>
     * <tr><td>TIMESTAMP     </td><td>{@link Date}</td></tr>
     * <tr><td>TIMEUUID      </td><td>{@link UUID}</td></tr>
     * <tr><td>TINYINT       </td><td>{@link Integer}</td></tr>
     * <tr><td>TUPLE         </td><td>{@link TupleValue}</td></tr>
     * <tr><td>UDT           </td><td>{@link UdtValue}</td></tr>
     * <tr><td>UUID          </td><td>{@link UUID}</td></tr>
     * <tr><td>VARCHAR       </td><td>{@link String}</td></tr>
     * <tr><td>VARINT        </td><td>{@link BigInteger}</td></tr>
     * </table>
     *
     * @return the Java class corresponding to this CQL type name.
     */
    public Class<?> asJavaClass() {
        return this.javaType;
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }

    /**
     * Gets the CQL name from a given {@link com.datastax.oss.driver.api.core.type.DataType} instance.
     *
     * @param dataType The data type.
     * @return The CQL name of the type.
     */
    public static String cqlName(@NonNull final com.datastax.oss.driver.api.core.type.DataType dataType) {
        return dataType.asCql(false, false);
    }
}



