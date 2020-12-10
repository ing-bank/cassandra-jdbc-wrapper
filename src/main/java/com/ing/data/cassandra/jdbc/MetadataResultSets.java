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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Map;

public class MetadataResultSets {
    private static final Logger log = LoggerFactory.getLogger(MetadataResultSets.class);
    public static final MetadataResultSets instance = new MetadataResultSets();
    static final String TABLE_CONSTANT = "TABLE";

    // Private Constructor
    private MetadataResultSets() {
    }

    public CassandraMetadataResultSet makeTableTypes(final CassandraStatement statement) throws SQLException {
        final ArrayList<MetadataRow> tableTypes = Lists.newArrayList();
        final MetadataRow row = new MetadataRow().addEntry("TABLE_TYPE", TABLE_CONSTANT);
        tableTypes.add(row);
        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(tableTypes));
    }

    public CassandraMetadataResultSet makeCatalogs(final CassandraStatement statement) throws SQLException {
        final ArrayList<MetadataRow> catalog = Lists.newArrayList();
        final MetadataRow row = new MetadataRow().addEntry("TABLE_CAT", statement.connection.getCatalog());
        catalog.add(row);
        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(catalog));
    }

    public CassandraMetadataResultSet makeSchemas(final CassandraStatement statement, String schemaPattern)
        throws SQLException {
        // TABLE_SCHEM String => schema name
        // TABLE_CATALOG String => catalog name (may be null)

        final ArrayList<MetadataRow> schemas = Lists.newArrayList();
        final Map<CqlIdentifier, KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();

        for (final Map.Entry<CqlIdentifier, KeyspaceMetadata> keyspace : keyspaces.entrySet()) {
            final KeyspaceMetadata keyspaceMetadata = keyspace.getValue();
            if ("%".equals(schemaPattern)) {
                schemaPattern = keyspaceMetadata.getName().asInternal();
            }
            if (schemaPattern == null || schemaPattern.equals(keyspaceMetadata.getName().asInternal())) {
                final MetadataRow row = new MetadataRow()
                    .addEntry("TABLE_SCHEM", keyspaceMetadata.getName().asInternal())
                    .addEntry("TABLE_CATALOG", statement.connection.getCatalog());
                schemas.add(row);
            }
        }

        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(schemas));
    }

    public CassandraMetadataResultSet makeTables(final CassandraStatement statement, String schemaPattern,
                                                 final String tableNamePattern) throws SQLException {
        //   1.   TABLE_CAT String => table catalog (may be null)
        //   2.   TABLE_SCHEM String => table schema (may be null)
        //   3.   TABLE_NAME String => table name
        //   4.   TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE",
        //   "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
        //   5.   REMARKS String => explanatory comment on the table
        //   6.   TYPE_CAT String => the types catalog (may be null)
        //   7.   TYPE_SCHEM String => the types schema (may be null)
        //   8.   TYPE_NAME String => type name (may be null)
        //   9.   SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table
        //   (may be null)
        //   10.  REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are
        //   "SYSTEM", "USER", "DERIVED". (may be null)

        final ArrayList<MetadataRow> schemas = Lists.newArrayList();
        final Map<CqlIdentifier, KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();

        for (final Map.Entry<CqlIdentifier, KeyspaceMetadata> keyspace : keyspaces.entrySet()) {
            final KeyspaceMetadata keyspaceMetadata = keyspace.getValue();
            if ("%".equals(schemaPattern)) {
                schemaPattern = keyspaceMetadata.getName().asInternal();
            }
            if (schemaPattern == null || schemaPattern.equals(keyspaceMetadata.getName().asInternal())) {
                final Map<CqlIdentifier, TableMetadata> tables = keyspaceMetadata.getTables();

                for (final Map.Entry<CqlIdentifier, TableMetadata> table : tables.entrySet()) {
                    final TableMetadata tableMetadata = table.getValue();
                    if ("%".equals(tableNamePattern) || tableNamePattern == null ||
                        tableNamePattern.equals(tableMetadata.getName().asInternal())) {
                        final MetadataRow row = new MetadataRow()
                            .addEntry("TABLE_CAT", statement.connection.getCatalog())
                            .addEntry("TABLE_SCHEM", keyspaceMetadata.getName().asInternal())
                            .addEntry("TABLE_NAME", tableMetadata.getName().asInternal())
                            .addEntry("TABLE_TYPE", TABLE_CONSTANT)
                            .addEntry("REMARKS", tableMetadata.getOptions()
                                .get(CqlIdentifier.fromCql("comment")).toString())
                            .addEntry("TYPE_CAT", null)
                            .addEntry("TYPE_SCHEM", null)
                            .addEntry("TYPE_NAME", null)
                            .addEntry("SELF_REFERENCING_COL_NAME", null)
                            .addEntry("REF_GENERATION", null);
                        schemas.add(row);
                    }
                }
            }
        }

        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(schemas));
    }

    public CassandraMetadataResultSet makeColumns(final CassandraStatement statement, String schemaPattern,
                                                  final String tableNamePattern, final String columnNamePattern)
        throws SQLException {
        final String originalSchemaPattern = schemaPattern;
        final ArrayList<MetadataRow> schemas = Lists.newArrayList();
        final Map<CqlIdentifier, KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();

        for (final Map.Entry<CqlIdentifier, KeyspaceMetadata> keyspace : keyspaces.entrySet()) {
            final  KeyspaceMetadata keyspaceMetadata = keyspace.getValue();
            if ("%".equals(originalSchemaPattern)) {
                schemaPattern = keyspaceMetadata.getName().asInternal();
            }

            if (schemaPattern == null || schemaPattern.equals(keyspaceMetadata.getName().asInternal())) {
                final Map<CqlIdentifier, TableMetadata> tables = keyspaceMetadata.getTables();

                for (final Map.Entry<CqlIdentifier, TableMetadata> table : tables.entrySet()) {
                    final TableMetadata tableMetadata = table.getValue();
                    if ("%".equals(tableNamePattern) || tableNamePattern == null ||
                        tableNamePattern.equals(tableMetadata.getName().asInternal())) {
                        final Map<CqlIdentifier, ColumnMetadata> columns = tableMetadata.getColumns();

                        int columnIndex = 1;
                        for (final Map.Entry<CqlIdentifier, ColumnMetadata> column : columns.entrySet()) {
                            final ColumnMetadata columnMetadata = column.getValue();
                            if ("%".equals(columnNamePattern) || columnNamePattern == null ||
                                columnNamePattern.equals(columnMetadata.getName().asInternal())) {
                                // COLUMN_SIZE
                                int length = -1;
                                final AbstractJdbcType jtype =
                                    TypesMap.getTypeForComparator(columnMetadata.getType().toString());

                                if (jtype instanceof JdbcBytes) {
                                    length = Integer.MAX_VALUE / 2;
                                }
                                if (jtype instanceof JdbcAscii || jtype instanceof JdbcUTF8) {
                                    length = Integer.MAX_VALUE;
                                }
                                if (jtype instanceof JdbcUUID) {
                                    length = 36;
                                }
                                if (jtype instanceof JdbcInt32) {
                                    length = 4;
                                }
                                if (jtype instanceof JdbcLong) {
                                    length = 8;
                                }

                                //NUM_PREC_RADIX
                                int npr = 2;
                                if (jtype != null && (jtype.getJdbcType() == Types.DECIMAL
                                    || jtype.getJdbcType() == Types.NUMERIC)) {
                                    npr = 10;
                                }

                                //CHAR_OCTET_LENGTH
                                final Integer charol = Integer.MAX_VALUE;

                                int jdbcType = Types.OTHER;
                                try {
                                    jdbcType = TypesMap.getTypeForComparator(columnMetadata.getType().toString())
                                        .getJdbcType();
                                } catch (final Exception e) {
                                    log.warn("Unable to get JDBC type for comparator ["
                                        + columnMetadata.getType().toString() + "]: " + e.getMessage());
                                }
                                final MetadataRow row = new MetadataRow()
                                    .addEntry("TABLE_CAT", statement.connection.getCatalog())
                                    .addEntry("TABLE_SCHEM", keyspaceMetadata.getName().asInternal())
                                    .addEntry("TABLE_NAME", tableMetadata.getName().asInternal())
                                    .addEntry("COLUMN_NAME", columnMetadata.getName().asInternal())
                                    .addEntry("DATA_TYPE", String.valueOf(jdbcType))
                                    .addEntry("TYPE_NAME", columnMetadata.getType().toString())
                                    .addEntry("COLUMN_SIZE", String.valueOf(length))
                                    .addEntry("BUFFER_LENGTH", "0")
                                    .addEntry("DECIMAL_DIGITS", null)
                                    .addEntry("NUM_PREC_RADIX", String.valueOf(npr))
                                    .addEntry("NULLABLE", String.valueOf(DatabaseMetaData.columnNoNulls))
                                    .addEntry("REMARKS", column.toString())
                                    .addEntry("COLUMN_DEF", null)
                                    .addEntry("SQL_DATA_TYPE", null)
                                    .addEntry("SQL_DATETIME_SUB", null)
                                    .addEntry("CHAR_OCTET_LENGTH", String.valueOf(charol))
                                    .addEntry("ORDINAL_POSITION", String.valueOf(columnIndex))
                                    .addEntry("IS_NULLABLE", "")
                                    .addEntry("SCOPE_CATALOG", null)
                                    .addEntry("SCOPE_SCHEMA", null)
                                    .addEntry("SCOPE_TABLE", null)
                                    .addEntry("SOURCE_DATA_TYPE", null)
                                    .addEntry("IS_AUTOINCREMENT", "NO")
                                    .addEntry("IS_GENERATEDCOLUMN", "NO");
                                schemas.add(row);
                                columnIndex++;
                            }
                        }
                    }
                }
            }
        }

        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(schemas));
        // 1.TABLE_CAT String => table catalog (may be null)
        // 2.TABLE_SCHEM String => table schema (may be null)
        // 3.TABLE_NAME String => table name
        // 4.COLUMN_NAME String => column name
        // 5.DATA_TYPE int => SQL type from java.sql.Types
        // 6.TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
        // 7.COLUMN_SIZE int => column size.
        // 8.BUFFER_LENGTH is not used.
        // 9.DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where
        // DECIMAL_DIGITS is not applicable.
        // 10.NUM_PREC_RADIX int => Radix (typically either 10 or 2)
        // 11.NULLABLE int => is NULL allowed. - columnNoNulls - might not allow NULL values
        // - columnNullable - definitely allows NULL values
        // - columnNullableUnknown - nullability unknown
        //
        // 12.REMARKS String => comment describing column (may be null)
        // 13.COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value
        // is enclosed in single quotes (may be null)
        // 14.SQL_DATA_TYPE int => unused
        // 15.SQL_DATETIME_SUB int => unused
        // 16.CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
        // 17.ORDINAL_POSITION int => index of column in table (starting at 1)
        // 18.IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
        // - YES --- if the parameter can include NULLs
        // - NO --- if the parameter cannot include NULLs
        // - empty string --- if the nullability for the parameter is unknown
        //
        // 19.SCOPE_CATALOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE
        // isn't REF)
        // 20.SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE
        // isn't REF)
        // 21.SCOPE_TABLE String => table name that this the scope of a reference attribure (null if the DATA_TYPE
        // isn't REF)
        // 22.SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from
        // java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
        // 23.IS_AUTOINCREMENT String => Indicates whether this column is auto incremented
        // - YES --- if the column is auto incremented
        // - NO --- if the column is not auto incremented
        // - empty string --- if it cannot be determined whether the column is auto incremented parameter is unknown
        // 24. IS_GENERATEDCOLUMN String => Indicates whether this is a generated column
        // - YES --- if this a generated column
        // - NO --- if this not a generated column
        // - empty string --- if it cannot be determined whether this is a generated column
    }

    public CassandraMetadataResultSet makeIndexes(final CassandraStatement statement, final String schema,
                                                  final String tableName,final  boolean unique,
                                                  final boolean approximate) throws SQLException {
        final ArrayList<MetadataRow> schemas = Lists.newArrayList();
        final Map<CqlIdentifier, KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();

        for (final Map.Entry<CqlIdentifier, KeyspaceMetadata> keyspace : keyspaces.entrySet()) {
            final KeyspaceMetadata keyspaceMetadata = keyspace.getValue();
            if (schema.equals(keyspaceMetadata.getName().asInternal())) {
                final Map<CqlIdentifier, TableMetadata> tables = keyspaceMetadata.getTables();

                for (final Map.Entry<CqlIdentifier, TableMetadata> table : tables.entrySet()) {
                    final TableMetadata tableMetadata = table.getValue();
                    if (tableName.equals(tableMetadata.getName().asInternal())) {
                        for (final Map.Entry<CqlIdentifier, IndexMetadata> index
                            : tableMetadata.getIndexes().entrySet()) {
                            final IndexMetadata indexMetadata = index.getValue();
                            final MetadataRow row = new MetadataRow()
                                .addEntry("TABLE_CAT", statement.connection.getCatalog())
                                .addEntry("TABLE_SCHEM", keyspaceMetadata.getName().asInternal())
                                .addEntry("TABLE_NAME", tableMetadata.getName().asInternal())
                                .addEntry("NON_UNIQUE", Boolean.TRUE.toString())
                                .addEntry("INDEX_QUALIFIER", statement.connection.getCatalog())
                                .addEntry("INDEX_NAME", indexMetadata.getName().asInternal())
                                .addEntry("TYPE", String.valueOf(DatabaseMetaData.tableIndexHashed))
                                .addEntry("ORDINAL_POSITION", "1")
                                .addEntry("COLUMN_NAME", indexMetadata.getTarget())
                                .addEntry("ASC_OR_DESC", null)
                                .addEntry("CARDINALITY", "-1")
                                .addEntry("PAGES", "-1")
                                .addEntry("FILTER_CONDITION", null);
                            schemas.add(row);
                        }
                    }
                }
            }
        }

        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(schemas));

        //1.TABLE_CAT String => table catalog (may be null)
        //2.TABLE_SCHEM String => table schema (may be null)
        //3.TABLE_NAME String => table name
        //4.NON_UNIQUE boolean => Can index values be non-unique. false when TYPE is tableIndexStatistic
        //5.INDEX_QUALIFIER String => index catalog (may be null); null when TYPE is tableIndexStatistic
        //6.INDEX_NAME String => index name; null when TYPE is tableIndexStatistic
        //7.TYPE short => index type: - tableIndexStatistic - this identifies table statistics that are returned in
        // conjunction with a table's index descriptions
        //- tableIndexClustered - this is a clustered index
        //- tableIndexHashed - this is a hashed index
        //- tableIndexOther - this is some other style of index
        //
        //8.ORDINAL_POSITION short => column sequence number within index; zero when TYPE is tableIndexStatistic
        //9.COLUMN_NAME String => column name; null when TYPE is tableIndexStatistic
        //10.ASC_OR_DESC String => column sort sequence, "A" => ascending, "D" => descending, may be null if sort
        // sequence is not supported; null when TYPE is tableIndexStatistic
        //11.CARDINALITY int => When TYPE is tableIndexStatistic, then this is the number of rows in the table;
        // otherwise, it is the number of unique values in the index.
        //12.PAGES int => When TYPE is tableIndexStatisic then this is the number of pages used for the table,
        // otherwise it is the number of pages used for the current index.
        //13.FILTER_CONDITION String => Filter condition, if any. (may be null)
    }

    public CassandraMetadataResultSet makePrimaryKeys(final CassandraStatement statement, final String schema,
                                                      final String tableName) throws SQLException {
        final ArrayList<MetadataRow> schemas = Lists.newArrayList();
        final Map<CqlIdentifier, KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();

        for (final Map.Entry<CqlIdentifier, KeyspaceMetadata> keyspace : keyspaces.entrySet()) {
            final KeyspaceMetadata keyspaceMetadata = keyspace.getValue();
            if (schema.equals(keyspaceMetadata.getName().asInternal())) {
                final Map<CqlIdentifier, TableMetadata> tables = keyspaceMetadata.getTables();

                for (final Map.Entry<CqlIdentifier, TableMetadata> table : tables.entrySet()) {
                    final TableMetadata tableMetadata = table.getValue();
                    if (tableName.equals(tableMetadata.getName().asInternal())) {
                        int seq = 0;
                        for (final ColumnMetadata col : tableMetadata.getPrimaryKey()) {
                            final MetadataRow row = new MetadataRow()
                                .addEntry("TABLE_CAT", statement.connection.getCatalog())
                                .addEntry("TABLE_SCHEM", keyspaceMetadata.getName().asInternal())
                                .addEntry("TABLE_NAME", tableMetadata.getName().asInternal())
                                .addEntry("COLUMN_NAME", col.getName().asInternal())
                                .addEntry("KEY_SEQ", String.valueOf(seq))
                                .addEntry("PK_NAME", null);
                            schemas.add(row);
                            seq++;
                        }
                    }
                }
            }
        }

        return new CassandraMetadataResultSet(statement, new MetadataResultSet().setRows(schemas));

        //1.TABLE_CAT String => table catalog (may be null)
        //2.TABLE_SCHEM String => table schema (may be null)
        //3.TABLE_NAME String => table name
        //4.COLUMN_NAME String => column name
        //5.KEY_SEQ short => sequence number within primary key( a value of 1 represents the first column of the
        // primary key, a value of 2 would represent the second column within the primary key).
        //6.PK_NAME String => primary key name (may be null)
    }

}
