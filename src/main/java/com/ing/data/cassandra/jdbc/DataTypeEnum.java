package com.ing.data.cassandra.jdbc;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.google.common.collect.Maps;
import org.apache.cassandra.db.marshal.CollectionType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public enum DataTypeEnum {

    ASCII(1, String.class, DataTypes.ASCII.asCql(false, false)),
    BIGINT(2, Long.class, DataTypes.BIGINT.asCql(false, false)),
    BLOB(3, ByteBuffer.class, DataTypes.BLOB.asCql(false, false)),
    BOOLEAN(4, Boolean.class, DataTypes.BOOLEAN.asCql(false, false)),
    COUNTER(5, Long.class, DataTypes.COUNTER.asCql(false, false)),
    DECIMAL(6, BigDecimal.class, DataTypes.DECIMAL.asCql(false, false)),
    DOUBLE(7, Double.class, DataTypes.DOUBLE.asCql(false, false)),
    FLOAT(8, Float.class, DataTypes.FLOAT.asCql(false, false)),
    INET(16, InetAddress.class, DataTypes.INET.asCql(false, false)),
    INT(9, Integer.class, DataTypes.INT.asCql(false, false)),
    TEXT(10, String.class, DataTypes.TEXT.asCql(false, false)),
    TIMESTAMP(11, Date.class, DataTypes.TIMESTAMP.asCql(false, false)),
    UUID(12, UUID.class, DataTypes.UUID.asCql(false, false)),
    VARCHAR(13, String.class, "VARCHAR"),
    VARINT(14, BigInteger.class, DataTypes.VARINT.asCql(false, false)),
    TIMEUUID(15, UUID.class, DataTypes.TIMEUUID.asCql(false, false)),
    LIST(32, List.class, CollectionType.Kind.LIST.name().toLowerCase()),
    SET(34, Set.class, CollectionType.Kind.SET.name().toLowerCase()),
    MAP(33, Map.class, CollectionType.Kind.MAP.name().toLowerCase()),
    UDT(48, UdtValue.class, "UDT"),
    TUPLE(49, TupleValue.class, "TUPLE"),
    CUSTOM(0, ByteBuffer.class, "CUSTOM"),
    SMALLINT(19, Integer.class, DataTypes.SMALLINT.asCql(false, false)),
    TINYINT(20, Integer.class, DataTypes.TINYINT.asCql(false, false)),
    DATE(17, Date.class, DataTypes.DATE.asCql(false, false)),
    TIME(18, Date.class, DataTypes.TIME.asCql(false, false));

    final int protocolId;
    final Class<?> javaType;
    final String cqlType;

    private static final DataTypeEnum[] nameToIds;
    private static final Map<String, DataTypeEnum> cqlDataTypeToDataType;

    static {

        cqlDataTypeToDataType = Maps.newHashMap();

        int maxCode = -1;
        for (final DataTypeEnum name : DataTypeEnum.values()) {
            maxCode = Math.max(maxCode, name.protocolId);
        }
        nameToIds = new DataTypeEnum[maxCode + 1];
        for (final DataTypeEnum name : DataTypeEnum.values()) {
            if (nameToIds[name.protocolId] != null)
                throw new IllegalStateException("Duplicate Id");
            nameToIds[name.protocolId] = name;

            cqlDataTypeToDataType.put(name.cqlType, name);
        }
    }

    private DataTypeEnum(final int protocolId, final Class<?> javaType, final String cqlType) {
        this.protocolId = protocolId;
        this.javaType = javaType;
        this.cqlType = cqlType;
    }

    static DataTypeEnum fromCqlTypeName(String cqlTypeName) {
        final int collectionTypeCharPos = cqlTypeName.indexOf("<");
        if (collectionTypeCharPos > 0) {
            cqlTypeName = cqlTypeName.substring(0, collectionTypeCharPos);
        }
        return cqlDataTypeToDataType.get(cqlTypeName);
    }

    static DataTypeEnum fromProtocolId(final int id) {
        final DataTypeEnum name = nameToIds[id];
        if (name == null)
            throw new IllegalArgumentException("Unknown data type protocol id: " + id);
        return name;
    }

    /**
     * Returns whether this data type name represent the name of a collection type
     * that is a list, set or map.
     *
     * @return whether this data type name represent the name of a collection type.
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
     * <p>
     * The correspondence between CQL types and java ones is as follow:
     * <table>
     * <caption>DataType to Java class correspondence</caption>
     * <tr><th>DataType (CQL)</th><th>Java Class</th></tr>
     * <tr><td>ASCII         </td><td>String</td></tr>
     * <tr><td>BIGINT        </td><td>Long</td></tr>
     * <tr><td>BLOB          </td><td>ByteBuffer</td></tr>
     * <tr><td>BOOLEAN       </td><td>Boolean</td></tr>
     * <tr><td>COUNTER       </td><td>Long</td></tr>
     * <tr><td>CUSTOM        </td><td>ByteBuffer</td></tr>
     * <tr><td>DECIMAL       </td><td>BigDecimal</td></tr>
     * <tr><td>DOUBLE        </td><td>Double</td></tr>
     * <tr><td>FLOAT         </td><td>Float</td></tr>
     * <tr><td>INET          </td><td>InetAddress</td></tr>
     * <tr><td>INT           </td><td>Integer</td></tr>
     * <tr><td>LIST          </td><td>List</td></tr>
     * <tr><td>MAP           </td><td>Map</td></tr>
     * <tr><td>SET           </td><td>Set</td></tr>
     * <tr><td>TEXT          </td><td>String</td></tr>
     * <tr><td>TIMESTAMP     </td><td>Date</td></tr>
     * <tr><td>UUID          </td><td>UUID</td></tr>
     * <tr><td>UDT           </td><td>UDTValue</td></tr>
     * <tr><td>TUPLE         </td><td>TupleValue</td></tr>
     * <tr><td>VARCHAR       </td><td>String</td></tr>
     * <tr><td>VARINT        </td><td>BigInteger</td></tr>
     * <tr><td>TIMEUUID      </td><td>UUID</td></tr>
     * </table>
     *
     * @return the java Class corresponding to this CQL type name.
     */
    public Class<?> asJavaClass() {
        return javaType;
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}



