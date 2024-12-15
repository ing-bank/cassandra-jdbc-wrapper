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

package com.ing.data.cassandra.jdbc.types;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.protocol.internal.ProtocolConstants.DataType;
import com.ing.data.cassandra.jdbc.CassandraConnection;
import com.ing.data.cassandra.jdbc.metadata.VersionedMetadata;
import org.semver4j.Semver;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static com.ing.data.cassandra.jdbc.utils.DriverUtil.CASSANDRA_5;

/**
 * Enumeration of CQL data types and the corresponding Java types.
 */
public enum DataTypeEnum implements VersionedMetadata {

    /**
     * {@code ascii} CQL type (type {@value DataType#ASCII} in CQL native protocol) mapped to {@link String} Java type.
     */
    ASCII(DataType.ASCII, String.class, cqlName(DataTypes.ASCII)),
    /**
     * {@code bigint} CQL type (type {@value DataType#BIGINT} in CQL native protocol) mapped to {@link Long} Java type.
     */
    BIGINT(DataType.BIGINT, Long.class, cqlName(DataTypes.BIGINT)),
    /**
     * {@code blob} CQL type (type {@value DataType#BLOB} in CQL native protocol) mapped to {@link ByteBuffer} Java
     * type.
     */
    BLOB(DataType.BLOB, ByteBuffer.class, cqlName(DataTypes.BLOB)),
    /**
     * {@code boolean} CQL type (type {@value DataType#BOOLEAN} in CQL native protocol) mapped to {@link Boolean} Java
     * type.
     */
    BOOLEAN(DataType.BOOLEAN, Boolean.class, cqlName(DataTypes.BOOLEAN)),
    /**
     * {@code counter} CQL type (type {@value DataType#COUNTER} in CQL native protocol) mapped to {@link Long} Java
     * type.
     */
    COUNTER(DataType.COUNTER, Long.class, cqlName(DataTypes.COUNTER)),
    /**
     * {@code custom} CQL type (type {@value DataType#CUSTOM} in CQL native protocol) mapped to {@link ByteBuffer} Java
     * type.
     */
    CUSTOM(DataType.CUSTOM, ByteBuffer.class, "CUSTOM", ifNotAwsKeyspaces()),
    /**
     * {@code date} CQL type (type {@value DataType#DATE} in CQL native protocol) mapped to {@link Date} Java type.
     */
    DATE(DataType.DATE, Date.class, cqlName(DataTypes.DATE), ifNotAwsKeyspaces()),
    /**
     * {@code decimal} CQL type (type {@value DataType#DECIMAL} in CQL native protocol) mapped to {@link BigDecimal}
     * Java type.
     */
    DECIMAL(DataType.DECIMAL, BigDecimal.class, cqlName(DataTypes.DECIMAL)),
    /**
     * {@code double} CQL type (type {@value DataType#DOUBLE} in CQL native protocol) mapped to {@link Double} Java
     * type.
     */
    DOUBLE(DataType.DOUBLE, Double.class, cqlName(DataTypes.DOUBLE)),
    /**
     * {@code duration} CQL type (type {@value DataType#DURATION} in CQL native protocol) mapped to {@link CqlDuration}
     * Java type.
     */
    DURATION(DataType.DURATION, CqlDuration.class, cqlName(DataTypes.DURATION), ifNotAwsKeyspaces()),
    /**
     * {@code float} CQL type (type {@value DataType#FLOAT} in CQL native protocol) mapped to {@link Float} Java type.
     */
    FLOAT(DataType.FLOAT, Float.class, cqlName(DataTypes.FLOAT)),
    /**
     * {@code inet} CQL type (type {@value DataType#INET} in CQL native protocol) mapped to {@link InetAddress} Java
     * type.
     */
    INET(DataType.INET, InetAddress.class, cqlName(DataTypes.INET)),
    /**
     * {@code int} CQL type (type {@value DataType#INT} in CQL native protocol) mapped to {@link Integer} Java type.
     */
    INT(DataType.INT, Integer.class, cqlName(DataTypes.INT)),
    /**
     * {@code list} CQL type (type {@value DataType#LIST} in CQL native protocol) mapped to {@link List} Java type.
     */
    LIST(DataType.LIST, List.class, "list"),
    /**
     * {@code map} CQL type (type {@value DataType#MAP} in CQL native protocol) mapped to {@link Map} Java type.
     */
    MAP(DataType.MAP, Map.class, "map"),
    /**
     * {@code set} CQL type (type {@value DataType#SET} in CQL native protocol) mapped to {@link Set} Java type.
     */
    SET(DataType.SET, Set.class, "set"),
    /**
     * {@code smallint} CQL type (type {@value DataType#SMALLINT} in CQL native protocol) mapped to {@link Short} Java
     * type.
     */
    SMALLINT(DataType.SMALLINT, Short.class, cqlName(DataTypes.SMALLINT), ifNotAwsKeyspaces()),
    /**
     * {@code text} CQL type (type {@value DataType#VARCHAR} in CQL native protocol) mapped to {@link String} Java type.
     */
    TEXT(DataType.VARCHAR, String.class, cqlName(DataTypes.TEXT)),
    /**
     * {@code time} CQL type (type {@value DataType#TIME} in CQL native protocol) mapped to {@link Time} Java type.
     */
    TIME(DataType.TIME, Time.class, cqlName(DataTypes.TIME), ifNotAwsKeyspaces()),
    /**
     * {@code timestamp} CQL type (type {@value DataType#TIMESTAMP} in CQL native protocol) mapped to {@link Timestamp}
     * Java type.
     */
    TIMESTAMP(DataType.TIMESTAMP, Timestamp.class, cqlName(DataTypes.TIMESTAMP)),
    /**
     * {@code timeuuid} CQL type (type {@value DataType#TIMEUUID} in CQL native protocol) mapped to {@link UUID} Java
     * type.
     */
    TIMEUUID(DataType.TIMEUUID, UUID.class, cqlName(DataTypes.TIMEUUID)),
    /**
     * {@code tinyint} CQL type (type {@value DataType#TINYINT} in CQL native protocol) mapped to {@link Byte} Java
     * type.
     */
    TINYINT(DataType.TINYINT, Byte.class, cqlName(DataTypes.TINYINT), ifNotAwsKeyspaces()),
    /**
     * {@code tuple} CQL type (type {@value DataType#TUPLE} in CQL native protocol) mapped to {@link TupleValue} Java
     * type.
     */
    TUPLE(DataType.TUPLE, TupleValue.class, "tuple", ifNotAwsKeyspaces()),
    /**
     * {@code udt} CQL type (type {@value DataType#UDT} in CQL native protocol) mapped to {@link UdtValue} Java type.
     */
    UDT(DataType.UDT, UdtValue.class, "UDT", ifNotAwsKeyspaces()),
    /**
     * {@code uuid} CQL type (type {@value DataType#UUID} in CQL native protocol) mapped to {@link UUID} Java type.
     */
    UUID(DataType.UUID, UUID.class, cqlName(DataTypes.UUID), ifNotAwsKeyspaces()),
    /**
     * {@code varchar} CQL type (type {@value DataType#VARCHAR} in CQL native protocol) mapped to {@link String} Java
     * type.
     */
    VARCHAR(DataType.VARCHAR, String.class, "VARCHAR"),
    /**
     * {@code varint} CQL type (type {@value DataType#VARINT} in CQL native protocol) mapped to {@link BigInteger} Java
     * type.
     */
    VARINT(DataType.VARINT, BigInteger.class, cqlName(DataTypes.VARINT)),
    /**
     * {@code vector} CQL type (type {@value DataType#LIST} in CQL native protocol) mapped to {@link CqlVector} Java
     * type.
     */
    VECTOR(DataType.LIST, CqlVector.class, "Vector", CASSANDRA_5, null, ifNotAwsKeyspaces());

    static final String VECTOR_CLASSNAME = "org.apache.cassandra.db.marshal.VectorType";

    private static final Map<String, DataTypeEnum> CQL_DATATYPE_TO_DATATYPE;

    /**
     * Gets the Java type corresponding to the enum value.
     */
    public final Class<?> javaType;
    /**
     * Gets the CQL type corresponding to the enum value.
     */
    public final String cqlType;

    final int protocolId;
    final Semver validFrom;
    final Semver invalidFrom;
    final Function<CassandraConnection, Boolean> additionalCondition;

    static {
        CQL_DATATYPE_TO_DATATYPE = new HashMap<>();
        for (final DataTypeEnum dataType : DataTypeEnum.values()) {
            CQL_DATATYPE_TO_DATATYPE.put(dataType.cqlType, dataType);
        }
    }

    /**
     * Constructs a {@code DataTypeEnum} item.
     *
     * @param protocolId          The type ID as defined in CQL binary protocol. (see
     *                            <a href="https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec">
     *                            CQL binary protocol definition</a> and {@link DataType}).
     * @param javaType            The corresponding Java type.
     * @param cqlType             The CQL type name.
     * @param validFrom           The minimal Cassandra version from which the CQL type exists. If {@code null}, we
     *                            consider the type exists in any version of the Cassandra database.
     * @param invalidFrom         The first Cassandra version in which the CQL type does not exist anymore. If
     *                            {@code null}, we consider the type exists in any version of the Cassandra database
     *                            greater than {@code validFrom}.
     * @param additionalCondition An additional condition to verify on the current connection to the database.
     */
    DataTypeEnum(final int protocolId, final Class<?> javaType, final String cqlType, final String validFrom,
                 final String invalidFrom, final Function<CassandraConnection, Boolean> additionalCondition) {
        this.protocolId = protocolId;
        this.javaType = javaType;
        this.cqlType = cqlType;
        this.validFrom = Semver.coerce(validFrom);
        this.invalidFrom = Semver.coerce(invalidFrom);
        this.additionalCondition = additionalCondition;
    }

    /**
     * Constructs a {@code DataTypeEnum} item valid from the specified version of Cassandra.
     *
     * @param protocolId The type ID as defined in CQL binary protocol. (see
     *                   <a href="https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec">
     *                   CQL binary protocol definition</a> and {@link DataType}).
     * @param javaType   The corresponding Java type.
     * @param cqlType    The CQL type name.
     * @param validFrom  The minimal Cassandra version from which the CQL type exists.
     */
    DataTypeEnum(final int protocolId, final Class<?> javaType, final String cqlType, final String validFrom) {
        this(protocolId, javaType, cqlType, validFrom, null, connection -> true);
    }

    /**
     * Constructs a {@code DataTypeEnum} item valid in any version of Cassandra.
     *
     * @param protocolId          The type ID as defined in CQL binary protocol. (see
     *                            <a href="https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec">
     *                            CQL binary protocol definition</a> and {@link DataType}).
     * @param javaType            The corresponding Java type.
     * @param cqlType             The CQL type name.
     * @param additionalCondition An additional condition to verify on the current connection to the database.
     */
    DataTypeEnum(final int protocolId, final Class<?> javaType, final String cqlType,
                 final Function<CassandraConnection, Boolean> additionalCondition) {
        this(protocolId, javaType, cqlType, null, null, additionalCondition);
    }

    /**
     * Constructs a {@code DataTypeEnum} item valid in any version of Cassandra.
     *
     * @param protocolId The type ID as defined in CQL binary protocol. (see
     *                   <a href="https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec">
     *                   CQL binary protocol definition</a> and {@link DataType}).
     * @param javaType   The corresponding Java type.
     * @param cqlType    The CQL type name.
     */
    DataTypeEnum(final int protocolId, final Class<?> javaType, final String cqlType) {
        this(protocolId, javaType, cqlType, (String) null);
    }

    /**
     * Gets an enumeration item from a CQL type name.
     *
     * @param cqlTypeName The CQL type name.
     * @return The enumeration item corresponding to the given CQL type name.
     */
    public static DataTypeEnum fromCqlTypeName(final String cqlTypeName) {
        // Manage user-defined types (e.g. "UDT(xxx)")
        if (cqlTypeName.startsWith(UDT.cqlType)) {
            return UDT;
        }
        // Manage vector type
        if (cqlTypeName.contains(VECTOR_CLASSNAME)) {
            return VECTOR;
        }
        // Manage collection types (e.g. "list<varchar>")
        final int collectionTypeCharPos = cqlTypeName.indexOf("<");
        String cqlDataType = cqlTypeName;
        if (collectionTypeCharPos > 0) {
            cqlDataType = cqlTypeName.substring(0, collectionTypeCharPos);
        }
        return CQL_DATATYPE_TO_DATATYPE.get(cqlDataType);
    }

    /**
     * Gets an enumeration item from a CQL data type.
     *
     * @param dataType The CQL data type.
     * @return The enumeration item corresponding to the given CQL data type.
     */
    public static DataTypeEnum fromDataType(final com.datastax.oss.driver.api.core.type.DataType dataType) {
        if (dataType instanceof UserDefinedType) {
            return UDT;
        }
        if (dataType instanceof VectorType) {
            return VECTOR;
        }
        return fromCqlTypeName(dataType.asCql(false, false));
    }

    /**
     * Returns whether this data type name represents the name of a collection type (i.e. that is a list, set, vector
     * or map).
     *
     * @return {@code true} if this data type name represents the name of a collection type, {@code false} otherwise.
     */
    public boolean isCollection() {
        switch (this) {
            case LIST:
            case SET:
            case MAP:
            case VECTOR:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns the Java Class corresponding to this CQL type name.
     * <br/>
     * The correspondence between CQL types and Java ones is as follows:
     * <table border="1">
     * <caption>DataType to Java class correspondence</caption>
     * <tr><th>CQL DataType</th><th>Java class</th></tr>
     * <tr><td>ASCII         </td><td>{@link String}</td></tr>
     * <tr><td>BIGINT        </td><td>{@link Long}</td></tr>
     * <tr><td>BLOB          </td><td>{@link ByteBuffer}</td></tr>
     * <tr><td>BOOLEAN       </td><td>{@link Boolean}</td></tr>
     * <tr><td>COUNTER       </td><td>{@link Long}</td></tr>
     * <tr><td>CUSTOM        </td><td>{@link ByteBuffer}</td></tr>
     * <tr><td>DATE          </td><td>{@link Date}</td></tr>
     * <tr><td>DECIMAL       </td><td>{@link BigDecimal}</td></tr>
     * <tr><td>DOUBLE        </td><td>{@link Double}</td></tr>
     * <tr><td>DURATION      </td><td>{@link CqlDuration}(*)</td></tr>
     * <tr><td>FLOAT         </td><td>{@link Float}</td></tr>
     * <tr><td>INET          </td><td>{@link InetAddress}</td></tr>
     * <tr><td>INT           </td><td>{@link Integer}</td></tr>
     * <tr><td>LIST          </td><td>{@link List}</td></tr>
     * <tr><td>MAP           </td><td>{@link Map}</td></tr>
     * <tr><td>SET           </td><td>{@link Set}</td></tr>
     * <tr><td>SMALLINT      </td><td>{@link Short}</td></tr>
     * <tr><td>TEXT          </td><td>{@link String}</td></tr>
     * <tr><td>TIME          </td><td>{@link Time}</td></tr>
     * <tr><td>TIMESTAMP     </td><td>{@link Timestamp}</td></tr>
     * <tr><td>TIMEUUID      </td><td>{@link UUID}</td></tr>
     * <tr><td>TINYINT       </td><td>{@link Byte}</td></tr>
     * <tr><td>TUPLE         </td><td>{@link TupleValue}</td></tr>
     * <tr><td>UDT           </td><td>{@link UdtValue}</td></tr>
     * <tr><td>UUID          </td><td>{@link UUID}</td></tr>
     * <tr><td>VARCHAR       </td><td>{@link String}</td></tr>
     * <tr><td>VARINT        </td><td>{@link BigInteger}</td></tr>
     * <tr><td>VECTOR        </td><td>{@link CqlVector}</td></tr>
     * </table>
     * <p>
     *     (*) See <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/temporal_types/">
     *         temporal types documentation</a> about the management of CQL durations in Java.
     * </p>
     *
     * @return the Java class corresponding to this CQL type name.
     */
    public Class<?> asJavaClass() {
        return this.javaType;
    }

    /**
     * Returns the CQL type name to lower case.
     *
     * @return The CQL type name to lower case.
     */
    public String asLowercaseCql() {
        return this.cqlType.toLowerCase();
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }

    @Override
    public String getName() {
        return this.cqlType;
    }

    @Override
    public Semver isValidFrom() {
        return this.validFrom;
    }

    @Override
    public Semver isInvalidFrom() {
        return this.invalidFrom;
    }

    @Override
    public boolean fulfillAdditionalCondition(final CassandraConnection connection) {
        return connection == null || this.additionalCondition.apply(connection);
    }

    /**
     * Gets the CQL name from a given {@link com.datastax.oss.driver.api.core.type.DataType} instance.
     * For vectors, dataType.asCql returns looks like 'org.apache.cassandra.db.marshal.VectorType(n)' where n is
     * the dimension of the vector. In this specific case, return a common name not including the dimension.
     *
     * @param dataType The data type.
     * @return The CQL name of the type.
     */
    public static String cqlName(@Nonnull final com.datastax.oss.driver.api.core.type.DataType dataType) {
        final String rawCql = dataType.asCql(false, false);
        if (rawCql.contains(VECTOR_CLASSNAME)) {
            return VECTOR.cqlType;
        }
        return rawCql;
    }

    /**
     * Some data types are not supported by Amazon Keyspaces, those are marked with an additional condition
     * ({@code ifNotAwsKeyspaces()}) in the enumerated data types. See
     * <a href="https://docs.aws.amazon.com/keyspaces/latest/devguide/cql.elements.html#cql.data-types"> data types
     * supported by Amazon Keyspaces</a>.
     * @return The function indicating if a given connection is bound to Amazon Keyspaces.
     */
    private static Function<CassandraConnection, Boolean> ifNotAwsKeyspaces() {
        return CassandraConnection::isNotConnectedToAmazonKeyspaces;
    }

}



