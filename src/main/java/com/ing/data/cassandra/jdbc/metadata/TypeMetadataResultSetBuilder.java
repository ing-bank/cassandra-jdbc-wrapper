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

package com.ing.data.cassandra.jdbc.metadata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.ing.data.cassandra.jdbc.CassandraMetadataResultSet;
import com.ing.data.cassandra.jdbc.CassandraStatement;
import com.ing.data.cassandra.jdbc.types.AbstractJdbcType;
import com.ing.data.cassandra.jdbc.types.DataTypeEnum;
import org.apache.commons.lang3.StringUtils;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.ing.data.cassandra.jdbc.types.AbstractJdbcType.DEFAULT_SCALE;
import static com.ing.data.cassandra.jdbc.types.TypesMap.getTypeForComparator;
import static java.sql.DatabaseMetaData.typeNullable;
import static java.sql.DatabaseMetaData.typePredBasic;
import static java.sql.Types.JAVA_OBJECT;

/**
 * Utility class building metadata result sets ({@link CassandraMetadataResultSet} objects) related to types.
 */
public class TypeMetadataResultSetBuilder extends AbstractMetadataResultSetBuilder {

    /**
     * Constructor.
     *
     * @param statement The statement.
     * @throws SQLException if a database access error occurs or this statement is closed.
     */
    public TypeMetadataResultSetBuilder(final CassandraStatement statement) throws SQLException {
        super(statement);
    }

    /**
     * Builds a valid result set of the description of the user-defined types (UDTs) defined in a particular schema.
     * This method is used to implement the method {@link DatabaseMetaData#getUDTs(String, String, String, int[])}.
     * <p>
     * Schema-specific UDTs in a Cassandra database will be considered as having type {@code JAVA_OBJECT}.
     * </p>
     * <p>
     * Only types matching the catalog, schema, type name and type criteria are returned. They are ordered by
     * {@code DATA_TYPE}, {@code TYPE_CAT}, {@code TYPE_SCHEM} and {@code TYPE_NAME}. The type name parameter may be
     * a fully-qualified name (it should respect the format {@code <SCHEMA_NAME>.<TYPE_NAME>}). In this case, the
     * {@code catalog} and {@code schemaPattern} parameters are ignored.
     * </p>
     * <p>
     * The columns of this result set are:
     * <ol>
     *     <li><b>TYPE_CAT</b> String => type's catalog, may be {@code null}: here is the Cassandra cluster name
     *     (if available).</li>
     *     <li><b>TYPE_SCHEM</b> String => type's schema, may be {@code null}: here is the keyspace the type is
     *     member of.</li>
     *     <li><b>TYPE_NAME</b> String => user-defined type name.</li>
     *     <li><b>CLASS_NAME</b> String => Java class name, always {@link UdtValue} in the current implementation.</li>
     *     <li><b>DATA_TYPE</b> int => type value defined in {@link Types}. One of {@link Types#JAVA_OBJECT},
     *     {@link Types#STRUCT}, or {@link Types#DISTINCT}. Always {@link Types#JAVA_OBJECT} in the current
     *     implementation.</li>
     *     <li><b>REMARKS</b> String => explanatory comment on the type, always empty in the current
     *     implementation.</li>
     *     <li><b>BASE_TYPE</b> short => type code of the source type of a {@code DISTINCT} type or the type that
     *     implements the user-generated reference type of the {@code SELF_REFERENCING_COLUMN} of a structured type
     *     as defined in {@link Types} ({@code null} if {@code DATA_TYPE} is not {@code DISTINCT} or not
     *     {@code STRUCT} with {@code REFERENCE_GENERATION = USER_DEFINED}). Always {@code null} in the current
     *     implementation.</li>
     * </ol>
     * </p>
     *
     * @param schemaPattern   A schema pattern name; must match the schema name as it is stored in the database;
     *                        {@code ""} retrieves those without a schema (will always return an empty set);
     *                        {@code null} means that the schema name should not be used to narrow the search and in
     *                        this case the search is restricted to the current schema (if available).
     * @param typeNamePattern A type name pattern; must match the type name as it is stored in the database (not
     *                        case-sensitive); may be a fully qualified name.
     * @param types           A list of user-defined types ({@link Types#JAVA_OBJECT}, {@link Types#STRUCT}, or
     *                        {@link Types#DISTINCT}) to include; {@code null} returns all types. All the UDTs defined
     *                        in a Cassandra database are considered as {@link Types#JAVA_OBJECT}, so other values will
     *                        return an empty result set.
     * @return A valid result set for implementation of {@link DatabaseMetaData#getUDTs(String, String, String, int[])}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet buildUDTs(final String schemaPattern, final String typeNamePattern,
                                                final int[] types) throws SQLException {
        final String catalog = this.connection.getCatalog();
        final ArrayList<MetadataRow> udtsRows = new ArrayList<>();

        // Parse the fully-qualified type name, if necessary.
        String schemaName = schemaPattern;
        final AtomicReference<String> typeName = new AtomicReference<>(typeNamePattern);
        if (typeNamePattern.contains(".")) {
            final String[] fullyQualifiedTypeNameParts = typeNamePattern.split("\\.");
            schemaName = fullyQualifiedTypeNameParts[0];
            typeName.set(fullyQualifiedTypeNameParts[1]);
        }

        filterBySchemaNamePattern(schemaName, keyspaceMetadata -> {
            final Map<CqlIdentifier, UserDefinedType> udts = keyspaceMetadata.getUserDefinedTypes();
            for (final Map.Entry<CqlIdentifier, UserDefinedType> udt : udts.entrySet()) {
                final UserDefinedType udtMetadata = udt.getValue();
                if (matchesPattern(typeName.get(), udtMetadata.getName().asInternal())
                    && (types == null || Arrays.stream(types).anyMatch(type -> type == JAVA_OBJECT))) {
                    final MetadataRow row = new MetadataRow()
                        .addEntry(TYPE_CATALOG, catalog)
                        .addEntry(TYPE_SCHEMA, keyspaceMetadata.getName().asInternal())
                        .addEntry(TYPE_NAME, udtMetadata.getName().asInternal())
                        .addEntry(CLASS_NAME, UdtValue.class.getName())
                        .addEntry(DATA_TYPE, String.valueOf(JAVA_OBJECT))
                        .addEntry(REMARKS, StringUtils.EMPTY)
                        .addEntry(BASE_TYPE, null);
                    udtsRows.add(row);
                }
            }
        }, null);

        // Results should all have the same DATA_TYPE and TYPE_CAT so just sort them by TYPE_SCHEM then TYPE_NAME.
        udtsRows.sort(Comparator.comparing(row -> ((MetadataRow) row).getString(TYPE_SCHEMA))
            .thenComparing(row -> ((MetadataRow) row).getString(TYPE_NAME)));
        return CassandraMetadataResultSet.buildFrom(this.statement, new MetadataResultSet().setRows(udtsRows));
    }

    /**
     * Builds a valid result set of all the data types supported by this database. This method is used to implement
     * the method {@link DatabaseMetaData#getTypeInfo()}.
     * <p>
     * They are ordered by DATA_TYPE and then by how closely the data type maps to the corresponding JDBC SQL type.
     * </p>
     * <p>
     * The Cassandra database does not support SQL distinct types. The information on the individual structured types
     * (considered as {@link Types#JAVA_OBJECT}, not {@link Types#STRUCT}) may be obtained from the {@code getUDTs()}
     * method.
     * </p>
     * <p>
     * The columns of this result set are:
     *     <ol>
     *         <li><b>TYPE_NAME</b> String => type name.</li>
     *         <li><b>DATA_TYPE</b> int => SQL data type from {@link Types}.</li>
     *         <li><b>PRECISION</b> int => maximum precision.</li>
     *         <li><b>LITERAL_PREFIX</b> String => prefix used to quote a literal (may be {@code null}).</li>
     *         <li><b>LITERAL_SUFFIX</b> String => suffix used to quote a literal (may be {@code null}).</li>
     *         <li><b>CREATE_PARAMS</b> String => parameters used in creating the type (may be {@code null}).</li>
     *         <li><b>NULLABLE</b> short => can you use {@code NULL} for this type:
     *              <ul>
     *                  <li>{@link DatabaseMetaData#typeNoNulls} - does not allow {@code NULL} values</li>
     *                  <li>{@link DatabaseMetaData#typeNullable} - allows {@code NULL} values</li>
     *                  <li>{@link DatabaseMetaData#typeNullableUnknown} - nullability unknown</li>
     *              </ul>
     *         </li>
     *         <li><b>CASE_SENSITIVE</b> boolean => is it case sensitive.</li>
     *         <li><b>SEARCHABLE</b> short => can you use "{@code WHERE}" based on this type:
     *              <ul>
     *                  <li>{@link DatabaseMetaData#typePredNone} - no support</li>
     *                  <li>{@link DatabaseMetaData#typePredChar} - only supported with {@code WHERE .. LIKE}</li>
     *                  <li>{@link DatabaseMetaData#typePredBasic} - supported except for {@code WHERE .. LIKE}</li>
     *                  <li>{@link DatabaseMetaData#typeSearchable} - supported for all {@code WHERE ..}</li>
     *              </ul>
     *         </li>
     *         <li><b>UNSIGNED_ATTRIBUTE</b> boolean => is it unsigned.</li>
     *         <li><b>FIXED_PREC_SCALE</b> boolean => can it be a money value.</li>
     *         <li><b>AUTO_INCREMENT</b> boolean => can it be used for an auto-increment value. Always {@code false}
     *         since Cassandra does not support auto-increment.</li>
     *         <li><b>LOCAL_TYPE_NAME</b> String => localized version of type name (may be {@code null}).</li>
     *         <li><b>MINIMUM_SCALE</b> short => minimum scale supported.</li>
     *         <li><b>MAXIMUM_SCALE</b> short => maximum scale supported.</li>
     *         <li><b>SQL_DATA_TYPE</b> int => not used.</li>
     *         <li><b>SQL_DATETIME_SUB</b> int => not used.</li>
     *         <li><b>NUM_PREC_RADIX</b> String => precision radix (typically either 10 or 2).</li>
     *     </ol>
     * </p>
     * <p>
     * The {@code PRECISION} column represents the maximum column size that the server supports for the given datatype.
     * For numeric data, this is the maximum precision. For character data, this is the length in characters. For
     * datetime data types, this is the length in characters of the {@code String} representation (assuming the maximum
     * allowed precision of the fractional seconds component). For binary data, this is the length in bytes.
     * For the {@code ROWID} datatype (not supported by Cassandra), this is the length in bytes. The value {@code null}
     * is returned for data types where the column size is not applicable.
     * </p>
     *
     * @return A valid result set for implementation of {@link DatabaseMetaData#getTypeInfo()}.
     * @throws SQLException when something went wrong during the creation of the result set.
     */
    public CassandraMetadataResultSet buildTypes() throws SQLException {
        final ArrayList<MetadataRow> types = new ArrayList<>();
        for (final DataTypeEnum dataType : DataTypeEnum.values()) {
            final AbstractJdbcType<?> jdbcType = getTypeForComparator(dataType.asLowercaseCql());
            String literalQuotingSymbol = null;
            if (jdbcType.needsQuotes()) {
                literalQuotingSymbol = "'";
            }
            final MetadataRow row = new MetadataRow()
                .addEntry(TYPE_NAME, dataType.cqlType)
                .addEntry(DATA_TYPE, String.valueOf(jdbcType.getJdbcType()))
                .addEntry(PRECISION, String.valueOf(jdbcType.getPrecision(null)))
                .addEntry(LITERAL_PREFIX, literalQuotingSymbol)
                .addEntry(LITERAL_SUFFIX, literalQuotingSymbol)
                .addEntry(CREATE_PARAMS, null)
                .addEntry(NULLABLE, String.valueOf(typeNullable)) // absence is the equivalent of null in Cassandra
                .addEntry(CASE_SENSITIVE, String.valueOf(jdbcType.isCaseSensitive()))
                .addEntry(SEARCHABLE, String.valueOf(typePredBasic))
                .addEntry(UNSIGNED_ATTRIBUTE, String.valueOf(!jdbcType.isSigned()))
                .addEntry(FIXED_PRECISION_SCALE, String.valueOf(!jdbcType.isCurrency()))
                .addEntry(AUTO_INCREMENT, String.valueOf(false))
                .addEntry(LOCALIZED_TYPE_NAME, null)
                .addEntry(MINIMUM_SCALE, String.valueOf(DEFAULT_SCALE))
                .addEntry(MAXIMUM_SCALE, String.valueOf(jdbcType.getScale(null)))
                .addEntry(SQL_DATA_TYPE, null)
                .addEntry(SQL_DATETIME_SUB, null)
                .addEntry(NUM_PRECISION_RADIX, String.valueOf(jdbcType.getPrecision(null)));
            types.add(row);
        }

        // Sort results by DATA_TYPE.
        types.sort(Comparator.comparing(row -> Integer.valueOf(row.getString(DATA_TYPE))));
        return CassandraMetadataResultSet.buildFrom(this.statement, new MetadataResultSet().setRows(types));
    }

}
