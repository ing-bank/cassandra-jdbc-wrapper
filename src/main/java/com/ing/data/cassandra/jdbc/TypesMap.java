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

import java.util.HashMap;
import java.util.Map;

/**
 * Mapping of Apache Cassandra CQL types to JDBC-equivalent types.
 */
public final class TypesMap {
    private static final Map<String, AbstractJdbcType<?>> TYPES_MAP = new HashMap<>();

    static {
        TYPES_MAP.put("org.apache.cassandra.db.marshal.AsciiType", JdbcAscii.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.BooleanType", JdbcBoolean.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.BytesType", JdbcBytes.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.ByteType", JdbcByte.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.CounterColumnType", JdbcCounterColumn.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.DateType", JdbcDate.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.DecimalType", JdbcDecimal.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.DoubleType", JdbcDouble.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.DurationType", JdbcDuration.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.FloatType", JdbcFloat.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.InetAddressType", JdbcInetAddress.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.IntegerType", JdbcInteger.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.Int32Type", JdbcInt32.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.LexicalUUIDType", JdbcLexicalUUID.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.LongType", JdbcLong.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.ShortType", JdbcShort.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.TimeType", JdbcTime.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.TimestampType", JdbcTimestamp.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.TimeUUIDType", JdbcTimeUUID.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.TupleType", JdbcTuple.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.UserType", JdbcUdt.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.UTF8Type", JdbcUTF8.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.UUIDType", JdbcUUID.INSTANCE);

        TYPES_MAP.put("org.apache.cassandra.db.marshal.ascii", JdbcAscii.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.bigint", JdbcLong.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.blob", JdbcBytes.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.boolean", JdbcBoolean.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.counter", JdbcLong.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.date", JdbcDate.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.decimal", JdbcDecimal.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.double", JdbcDouble.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.duration", JdbcDuration.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.float", JdbcFloat.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.inet", JdbcInetAddress.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.int", JdbcInt32.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.smallint", JdbcShort.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.text", JdbcUTF8.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.time", JdbcTime.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.timestamp", JdbcTimestamp.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.timeuuid", JdbcTimeUUID.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.tinyint", JdbcByte.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.tuple", JdbcTuple.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.udt", JdbcUdt.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.uuid", JdbcUUID.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.varchar", JdbcUTF8.INSTANCE);
        TYPES_MAP.put("org.apache.cassandra.db.marshal.varint", JdbcInteger.INSTANCE);
    }

    private TypesMap() {
        // Private constructor to hide the public one.
    }

    /**
     * Gets the JDBC-equivalent type for the given CQL type.
     *
     * @param comparator The CQL type.
     * @return The JDBC-equivalent type for the given CQL type.
     */
    public static AbstractJdbcType<?> getTypeForComparator(final String comparator) {
        // If not fully qualified, assume it's the short name for a built-in type.
        if (comparator != null && !comparator.contains(".")) {
            return TYPES_MAP.getOrDefault("org.apache.cassandra.db.marshal." + comparator.toLowerCase(),
                JdbcOther.INSTANCE);
        }
        return TYPES_MAP.getOrDefault(comparator, JdbcOther.INSTANCE);
    }
}
