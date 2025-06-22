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
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ing.data.cassandra.jdbc.CassandraConnection;
import com.ing.data.cassandra.jdbc.CassandraMetadataResultSet;
import com.ing.data.cassandra.jdbc.CassandraStatement;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

/**
 * Abstract class to implement for each utility class building database metadata result sets
 * ({@link CassandraMetadataResultSet} objects).
 */
public abstract class AbstractMetadataResultSetBuilder {

    static final String TABLE = "TABLE";
    static final String CQL_OPTION_COMMENT = "comment";
    static final String ASC_OR_DESC = "ASC_OR_DESC";
    static final String ATTRIBUTE_DEFAULT = "ATTR_DEF";
    static final String ATTRIBUTE_NAME = "ATTR_NAME";
    static final String ATTRIBUTE_SIZE = "ATTR_SIZE";
    static final String ATTRIBUTE_TYPE_NAME = "ATTR_TYPE_NAME";
    static final String AUTO_INCREMENT = "AUTO_INCREMENT";
    static final String BASE_TYPE = "BASE_TYPE";
    static final String BUFFER_LENGTH = "BUFFER_LENGTH";
    static final String CARDINALITY = "CARDINALITY";
    static final String CASE_SENSITIVE = "CASE_SENSITIVE";
    static final String CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH";
    static final String CLASS_NAME = "CLASS_NAME";
    static final String COLUMN_DEFAULT = "COLUMN_DEF";
    static final String COLUMN_NAME = "COLUMN_NAME";
    static final String COLUMN_SIZE = "COLUMN_SIZE";
    static final String COLUMN_TYPE = "COLUMN_TYPE";
    static final String CREATE_PARAMS = "CREATE_PARAMS";
    static final String DATA_TYPE = "DATA_TYPE";
    static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
    static final String FILTER_CONDITION = "FILTER_CONDITION";
    static final String FIXED_PRECISION_SCALE = "FIXED_PREC_SCALE";
    static final String FUNCTION_CATALOG = "FUNCTION_CAT";
    static final String FUNCTION_NAME = "FUNCTION_NAME";
    static final String FUNCTION_SCHEMA = "FUNCTION_SCHEM";
    static final String FUNCTION_TYPE = "FUNCTION_TYPE";
    static final String INDEX_NAME = "INDEX_NAME";
    static final String INDEX_QUALIFIER = "INDEX_QUALIFIER";
    static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
    static final String IS_GENERATED_COLUMN = "IS_GENERATEDCOLUMN";
    static final String IS_NULLABLE = "IS_NULLABLE";
    static final String KEY_SEQ = "KEY_SEQ";
    static final String LENGTH = "LENGTH";
    static final String LITERAL_PREFIX = "LITERAL_PREFIX";
    static final String LITERAL_SUFFIX = "LITERAL_SUFFIX";
    static final String LOCALIZED_TYPE_NAME = "LOCAL_TYPE_NAME";
    static final String MAXIMUM_SCALE = "MAXIMUM_SCALE";
    static final String MINIMUM_SCALE = "MINIMUM_SCALE";
    static final String NO_VALUE = "NO";
    static final String NON_UNIQUE = "NON_UNIQUE";
    static final String NULLABLE = "NULLABLE";
    static final String NUM_PRECISION_RADIX = "NUM_PREC_RADIX";
    static final String ORDINAL_POSITION = "ORDINAL_POSITION";
    static final String PAGES = "PAGES";
    static final String PRECISION = "PRECISION";
    static final String PRIMARY_KEY_NAME = "PK_NAME";
    static final String PSEUDO_COLUMN = "PSEUDO_COLUMN";
    static final String RADIX = "RADIX";
    static final String REF_GENERATION = "REF_GENERATION";
    static final String REMARKS = "REMARKS";
    static final String SCALE = "SCALE";
    static final String SCOPE = "SCOPE";
    static final String SCOPE_CATALOG = "SCOPE_CATALOG";
    static final String SCOPE_SCHEMA = "SCOPE_SCHEMA";
    static final String SCOPE_TABLE = "SCOPE_TABLE";
    static final String SEARCHABLE = "SEARCHABLE";
    static final String SELF_REFERENCING_COL_NAME = "SELF_REFERENCING_COL_NAME";
    static final String SOURCE_DATA_TYPE = "SOURCE_DATA_TYPE";
    static final String SPECIFIC_NAME = "SPECIFIC_NAME";
    static final String SQL_DATA_TYPE = "SQL_DATA_TYPE";
    static final String SQL_DATETIME_SUB = "SQL_DATETIME_SUB";
    static final String TABLE_CATALOG_SHORTNAME = "TABLE_CAT";
    static final String TABLE_CATALOG = "TABLE_CATALOG";
    static final String TABLE_NAME = "TABLE_NAME";
    static final String TABLE_SCHEMA = "TABLE_SCHEM";
    static final String TABLE_TYPE = "TABLE_TYPE";
    static final String TYPE = "TYPE";
    static final String TYPE_CATALOG = "TYPE_CAT";
    static final String TYPE_NAME = "TYPE_NAME";
    static final String TYPE_SCHEMA = "TYPE_SCHEM";
    static final String UNSIGNED_ATTRIBUTE = "UNSIGNED_ATTRIBUTE";
    static final String YES_VALUE = "YES";

    CassandraStatement statement;
    CassandraConnection connection;

    /**
     * Abstract constructor.
     *
     * @param statement The statement.
     * @throws SQLException if a database access error occurs or this statement is closed.
     */
    protected AbstractMetadataResultSetBuilder(final CassandraStatement statement) throws SQLException {
        this.statement = statement;
        this.connection = statement.getCassandraConnection();
    }

    /**
     * Checks whether the specified pattern (potentially using SQL wildcard '%') matches the given string.
     * <p>
     *     For example, the pattern {@code a%_table} will be transformed to the regular expression {@code ^a.*_table$}
     *     and will match {@code any_table} but not {@code main_table}.
     * </p>
     *
     * @param pattern     The SQL pattern to check.
     * @param testedValue The tested string.
     * @return {@code true} if the pattern matches the tested string, {@code false} otherwise.
     */
    public boolean matchesPattern(final String pattern, final String testedValue) {
        return testedValue.matches(String.format("(?i)^%s$", pattern.replace("%", ".*")));
    }

    /**
     * Executes a {@link Consumer} function on each {@link KeyspaceMetadata} instance corresponding to a keyspace
     * matching the specified schema name pattern.
     *
     * @implNote The pattern matches a keyspace name if the pattern is {@code null} or empty or if the function
     * {@link #matchesPattern(String, String)} returns {@code true}.
     * @param schemaNamePattern The schema name pattern.
     * @param consumer          The applied consumer function when there is a pattern match.
     * @param altConsumer       The applied consumer function when there is no pattern match. If {@code null}, a no-op
     *                          consumer is used.
     */
    @SuppressWarnings("SameParameterValue")
    void filterBySchemaNamePattern(final String schemaNamePattern, final Consumer<KeyspaceMetadata> consumer,
                                   final Consumer<KeyspaceMetadata> altConsumer) {
        filterByPattern(schemaNamePattern, this.connection.getClusterMetadata().getKeyspaces(),
            (pattern, keyspaceMetadata) -> pattern == null || StringUtils.EMPTY.equals(pattern)
                || matchesPattern(pattern, keyspaceMetadata.getName().asInternal()), consumer,
            Optional.ofNullable(altConsumer).orElse(noOpConsumer()));
    }

    /**
     * Executes a {@link Consumer} function on each {@link TableMetadata} instance corresponding to a table
     * matching the specified table name pattern in the specified keyspace.
     *
     * @implNote The pattern matches a table name if the pattern is {@code null} or if the function
     * {@link #matchesPattern(String, String)} returns {@code true}.
     * @param tableNamePattern The table name pattern.
     * @param keyspaceMetadata The keyspace on which the filter is applied: only the tables present in this keyspace
     *                         are filtered.
     * @param consumer          The applied consumer function when there is a pattern match.
     * @param altConsumer       The applied consumer function when there is no pattern match. If {@code null}, a no-op
     *                          consumer is used.
     */
    @SuppressWarnings("SameParameterValue")
    void filterByTableNamePattern(final String tableNamePattern, final KeyspaceMetadata keyspaceMetadata,
                                  final Consumer<TableMetadata> consumer, final Consumer<TableMetadata> altConsumer) {
        filterByPattern(tableNamePattern, keyspaceMetadata.getTables(),
            (pattern, tableMetadata) -> pattern == null
                || matchesPattern(pattern, tableMetadata.getName().asInternal()), consumer,
            Optional.ofNullable(altConsumer).orElse(noOpConsumer()));
    }

    /**
     * Executes a {@link Consumer} function on each {@link ColumnMetadata} instance corresponding to a column
     * matching the specified column name pattern in the specified table.
     *
     * @implNote The pattern matches a column name if the pattern is {@code null} or if the function
     * {@link #matchesPattern(String, String)} returns {@code true}.
     * @param columnNamePattern The column name pattern.
     * @param tableMetadata     The table on which the filter is applied: only the columns present in this table
     *                          are filtered.
     * @param consumer          The applied consumer function when there is a pattern match.
     * @param altConsumer       The applied consumer function when there is no pattern match. If {@code null}, a no-op
     *                          consumer is used.
     */
    void filterByColumnNamePattern(final String columnNamePattern, final TableMetadata tableMetadata,
                                   final Consumer<ColumnMetadata> consumer,
                                   final Consumer<ColumnMetadata> altConsumer) {
        filterByPattern(columnNamePattern, tableMetadata.getColumns(),
            (pattern, columnMetadata) -> pattern == null
                || matchesPattern(pattern, columnMetadata.getName().asInternal()), consumer,
            Optional.ofNullable(altConsumer).orElse(noOpConsumer()));
    }

    /**
     * Executes a {@link BiConsumer} function on each pair of {@link FunctionSignature} and {@link FunctionMetadata}
     * instances corresponding to a function matching the specified function name pattern in the specified keyspace.
     *
     * @implNote The pattern matches a function name if the pattern is {@code null} or if the function
     * {@link #matchesPattern(String, String)} returns {@code true}.
     * @param functionNamePattern The function name pattern.
     * @param keyspaceMetadata    The keyspace on which the filter is applied: only the functions present in this
     *                            keyspace are filtered.
     * @param consumer            The applied bi-consumer function when there is a pattern match.
     */
    void filterByFunctionNamePattern(final String functionNamePattern, final KeyspaceMetadata keyspaceMetadata,
                                     final BiConsumer<FunctionSignature, FunctionMetadata> consumer) {
        for (final Map.Entry<FunctionSignature, FunctionMetadata> entry : keyspaceMetadata.getFunctions().entrySet()) {
            final FunctionSignature functionSignature = entry.getKey();
            final FunctionMetadata functionMetadata = entry.getValue();
            if (functionNamePattern == null
                || matchesPattern(functionNamePattern, functionSignature.getName().asInternal())) {
                consumer.accept(functionSignature, functionMetadata);
            }
        }
    }

    <M> void filterByPattern(final String pattern, final Map<CqlIdentifier, M> metadataMap,
                             final BiPredicate<String, M> patternMatcher,
                             final Consumer<M> consumer, final Consumer<M> altConsumer) {
        for (final Map.Entry<CqlIdentifier, M> entry : metadataMap.entrySet()) {
            final M entityMetadata = entry.getValue();
            if (patternMatcher.test(pattern, entityMetadata)) {
                consumer.accept(entityMetadata);
            } else {
                altConsumer.accept(entityMetadata);
            }
        }
    }

    <M> Consumer<M> noOpConsumer() {
        return entityMetadata -> {
            // No-op consumer: do nothing
        };
    }

}
