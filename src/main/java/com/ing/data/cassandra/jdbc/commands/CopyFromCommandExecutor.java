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

package com.ing.data.cassandra.jdbc.commands;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.ing.data.cassandra.jdbc.CassandraConnection;
import com.ing.data.cassandra.jdbc.CassandraStatement;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.ICSVParser;
import com.opencsv.exceptions.CsvException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.LOG;
import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.buildEmptyResultSet;
import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.translateFilename;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.COMMA;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.SINGLE_QUOTE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.CANNOT_READ_CSV_FILE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.COLUMN_DEFINITIONS_RETRIEVAL_FAILED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.CSV_FILE_NOT_FOUND;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.MISSING_COLUMN_DEFINITIONS;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.wrap;

/**
 * Executor for special command importing data into a table from a CSV file.
 * <p>
 *     {@code COPY <tableName>[(<columns>)] FROM <origin>[ WITH <options>[ AND <options> ...]]}: where {@code tableName}
 *     is the name of the table into which the rows are inserted (it may be prefixed with the keyspace name), *
 *     {@code columns} is a subset of columns to import from the CSV file specified by adding a comma-separated list of
 *     column names, {@code origin} is a string literal (with single quotes) representing the path to the imported file;
 *     and {@code options} are the options among the following:
 *     <ul>
 *         <li>{@code BATCHSIZE}: the number of rows inserted in a single batch.
 *         Defaults to {@value #DEFAULT_BATCH_SIZE}.</li>
 *         <li>{@code DECIMALSEP}: the character that is used as the decimal point separator.
 *         Defaults to {@value #DEFAULT_DECIMAL_SEPARATOR}.</li>
 *         <li>{@code DELIMITER}: the character that is used to separate fields.
 *         Defaults to {@value #DEFAULT_DELIMITER_CHAR}.</li>
 *         <li>{@code ESCAPE}: the character that is used to escape the literal uses of the {@code QUOTE} character.
 *         Defaults to {@value #DEFAULT_ESCAPE_CHAR}.</li>
 *         <li>{@code HEADER}: whether the first line in the CSV input file will contain the column names.
 *         Defaults to {@code false}.</li>
 *         <li>{@code MAXROWS}: the maximum number of rows to import, negative meaning unlimited.
 *         Defaults to {@value #DEFAULT_MAX_ROWS}.</li>
 *         <li>{@code NULLVAL}: the string placeholder for null values. Defaults to {@value #DEFAULT_NULL_FORMAT}.</li>
 *         <li>{@code SKIPCOLS}: a comma-separated list of column names to ignore.
 *         By default, no columns are skipped.</li>
 *         <li>{@code SKIPROWS}: the number of initial rows to skip. Defaults to {@value #DEFAULT_SKIPPED_ROWS}.</li>
 *         <li>{@code QUOTE}: the character that is used to enclose field values.
 *         Defaults to {@value #DEFAULT_QUOTE_CHAR}.</li>
 *         <li>{@code THOUSANDSSEP}: the character that is used to separate thousands.
 *         Defaults to the empty string.</li>
 *     </ul>
 * </p>
 * <p>
 *     If the path to the imported file is not absolute, it is interpreted relative to the current working directory.
 *     The tilde shorthand notation ({@code '~/dir'}) is supported for referring to the home directory.
 * </p>
 * <p>
 *     The format to define the value of an option is: {@code <optionName> = <value>}. The value must be quoted if it
 *     is a string literal.
 * </p>
 * <p>
 *     The following options are not supported:
 *     <ul>
 *         <li>{@code BOOLSTYLE}</li>
 *         <li>{@code CHUNKSIZE}</li>
 *         <li>{@code CONFIGFILE}</li>
 *         <li>{@code DATETIMEFORMAT}</li>
 *         <li>{@code ERRFILE}</li>
 *         <li>{@code INGESTRATE}</li>
 *         <li>{@code MAXATTEMPTS}</li>
 *         <li>{@code MAXBATCHSIZE} (use {@code BATCHSIZE} instead)</li>
 *         <li>{@code MAXINSERTERRORS}</li>
 *         <li>{@code MAXPARSEERRORS}</li>
 *         <li>{@code MINBATCHSIZE} (use {@code BATCHSIZE} instead)</li>
 *         <li>{@code NULL} (use {@code NULLVAL} instead)</li>
 *         <li>{@code NUMPROCESSES}</li>
 *         <li>{@code RATEFILE}</li>
 *         <li>{@code REPORTFREQUENCY}</li>
 *         <li>{@code TTL}</li>
 *     </ul>
 *     Using unknown options will throw a {@link SQLSyntaxErrorException}.
 * </p>
 * <p>
 *     The documentation of the original {@code COPY FROM} command is available:
 *     <ul>
 *         <li><a href="https://cassandra.apache.org/doc/latest/cassandra/managing/tools/cqlsh.html#copy-from">
 *             in the Apache Cassandra® documentation</a></li>
 *         <li><a href="https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlshCopy.html">
 *             in the DataStax CQL reference documentation</a></li>
 *     </ul>
 * </p>
 * @implNote <p>
 *     The special value {@code STDIN} for the {@code origin} parameter, used to import the CSV content from the
 *     standard input, is not supported.
 * </p>
 * <p>
 *     Insertions in date, time and timestamp columns expect the format used in the imported CSV file follows the
 *     Cassandra CQL standard for these types, i.e. respectively: {@value DEFAULT_DATE_FORMAT},
 *     {@value DEFAULT_TIME_FORMAT} and {@value DEFAULT_DATETIME_FORMAT}.
 * </p>
 */
public class CopyFromCommandExecutor extends AbstractCopyCommandExecutor {

    private static final int DEFAULT_BATCH_SIZE = 20;
    private static final int DEFAULT_MAX_ROWS = -1;
    private static final int DEFAULT_SKIPPED_ROWS = 0;

    // Supported options (not in common with COPY TO)
    private static final String OPTION_BATCHSIZE = "BATCHSIZE";
    private static final String OPTION_MAXROWS = "MAXROWS";
    private static final String OPTION_SKIPCOLS = "SKIPCOLS";
    private static final String OPTION_SKIPROWS = "SKIPROWS";

    private final String origin;
    private Map<String, Integer> columnsTypesInTargetTable = new HashMap<>();

    /**
     * Constructor.
     *
     * @param tableName The parameter {@code tableName} of the command.
     * @param columns   The optional parameter {@code columns} of the command.
     * @param origin    The parameter {@code origin} of the command.
     * @param options   The optional parameter {@code options} of the command already parsed into a {@link Properties}
     *                  instance.
     * @throws SQLSyntaxErrorException if an unknown option is used.
     */
    public CopyFromCommandExecutor(@Nonnull final String tableName, final String columns, @Nonnull final String origin,
                                   @Nonnull final Properties options) throws SQLSyntaxErrorException {
        this.tableName = tableName;
        this.columns = StringUtils.defaultIfBlank(columns, EMPTY);
        this.origin = origin;
        this.options = options;

        SUPPORTED_OPTIONS.add(OPTION_BATCHSIZE);
        SUPPORTED_OPTIONS.add(OPTION_MAXROWS);
        SUPPORTED_OPTIONS.add(OPTION_SKIPCOLS);
        SUPPORTED_OPTIONS.add(OPTION_SKIPROWS);
        checkOptions();

        configureFormatters();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(final CassandraStatement statement, final String cql) throws SQLException {
        final CassandraConnection connection = statement.getCassandraConnection();
        this.columnsTypesInTargetTable = getColumnsTypesInTargetTable(connection);
        if (this.columnsTypesInTargetTable.isEmpty()) {
            throw new SQLException(MISSING_COLUMN_DEFINITIONS);
        }
        setTargetColumnsIfNotSpecified();

        final Statement insertStatement = connection.createStatement();

        final boolean hasHeaders = Boolean.parseBoolean(this.options.getProperty(OPTION_HEADER));
        final Set<String> skippedColumns = Arrays.stream(((String) this.options.getOrDefault(OPTION_SKIPCOLS, EMPTY))
                .split(COMMA))
            .map(String::trim)
            .collect(Collectors.toSet());
        final int batchSize = getOptionValueAsInt(OPTION_BATCHSIZE, DEFAULT_BATCH_SIZE);

        CSVReader csvReader = null;
        try {
            csvReader = new CSVReaderBuilder(new FileReader(translateFilename(this.origin)))
                .withSkipLines(getOptionValueAsInt(OPTION_SKIPROWS, DEFAULT_SKIPPED_ROWS))
                .withCSVParser(configureCsvParser())
                .build();
            final List<String[]> allRows = csvReader.readAll();

            final Map<Integer, String> csvColumns;
            int startRow = 0;
            if (hasHeaders) {
                csvColumns = mapColumnsFromArray(allRows.get(0));
                startRow = 1;
            } else {
                csvColumns = mapColumnsFromArray(this.columns.split(COMMA));
            }

            int maxRows = getOptionValueAsInt(OPTION_MAXROWS, DEFAULT_MAX_ROWS);
            if (maxRows < 0) {
                maxRows = allRows.size();
            }
            for (int i = startRow; i < maxRows; i++) {
                handleValuesToInsert(insertStatement, allRows.get(i), csvColumns, getColumnsToImport(skippedColumns));
                final int rowsInBatch = i - startRow + 1;
                if (rowsInBatch % batchSize == 0 || rowsInBatch == maxRows - startRow) {
                    insertStatement.executeBatch();
                }
            }
        } catch (final FileNotFoundException e) {
            throw new SQLException(String.format(CSV_FILE_NOT_FOUND, this.origin), e);
        } catch (final IOException | CsvException e) {
            throw new SQLException(String.format(CANNOT_READ_CSV_FILE, this.origin, e.getMessage()), e);
        } finally {
            insertStatement.close();
            IOUtils.closeQuietly(csvReader);
        }

        return buildEmptyResultSet();
    }

    private ICSVParser configureCsvParser() {
        return new CSVParserBuilder()
            .withQuoteChar(getOptionValueAsChar(OPTION_QUOTE, DEFAULT_QUOTE_CHAR))
            .withSeparator(getOptionValueAsChar(OPTION_DELIMITER, DEFAULT_DELIMITER_CHAR))
            .withEscapeChar(getOptionValueAsChar(OPTION_ESCAPE, DEFAULT_ESCAPE_CHAR))
            .build();
    }

    private Map<String, Integer> getColumnsTypesInTargetTable(final CassandraConnection connection)
        throws SQLException {
        // We use a LinkedHashMap to preserve the order of the columns determined by the metadata ORDINAL_POSITION
        // (the columns returned by the method 'getColumns()' respect this order).
        final Map<String, Integer> columnsTypes = new LinkedHashMap<>();
        try {
            String keyspaceName = connection.getSchema();
            String tableName = this.tableName;
            if (this.tableName.contains(".")) {
                final String[] keyspaceAndTable = this.tableName.split("\\.");
                keyspaceName = keyspaceAndTable[0];
                tableName = keyspaceAndTable[1];
            }
            final java.sql.ResultSet columnsMetadataRs = connection.getMetaData()
                .getColumns(null, keyspaceName, tableName, null);

            while (columnsMetadataRs.next()) {
                columnsTypes.put(columnsMetadataRs.getString("COLUMN_NAME"), columnsMetadataRs.getInt("DATA_TYPE"));
            }
            columnsMetadataRs.close();
        } catch (final SQLException e) {
            throw new SQLException(COLUMN_DEFINITIONS_RETRIEVAL_FAILED, e);
        }
        return columnsTypes;
    }

    private void setTargetColumnsIfNotSpecified() {
        if (StringUtils.isBlank(this.columns)) {
            this.columns = String.join(COMMA, this.columnsTypesInTargetTable.keySet());
        }
    }

    private List<String> getColumnsToImport(final Set<String> skippedColumns) {
        return Arrays.stream(this.columns.split(COMMA))
            .map(String::trim)
            .filter(col -> !skippedColumns.contains(col))
            .collect(Collectors.toList());
    }

    private Map<Integer, String> mapColumnsFromArray(final String[] columnsNames) {
        final Map<Integer, String> csvColumns = new LinkedHashMap<>();
        for (int i = 0; i < columnsNames.length; i++) {
            csvColumns.put(i, columnsNames[i]);
        }
        return csvColumns;
    }

    private void handleValuesToInsert(final Statement statement,
                                      final String[] row,
                                      final Map<Integer, String> csvColumns,
                                      final List<String> columnsToImport) {
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < row.length; i++) {
            final String csvColumnName = csvColumns.get(i);
            if (columnsToImport.contains(csvColumnName)) {
                final int targetJdbcType = this.columnsTypesInTargetTable.get(csvColumnName);
                final String o = parseValue(row[i], targetJdbcType);
                values.add(o);
            }
        }
        final String cql = String.format("INSERT INTO %s(%s) VALUES (%s)", this.tableName,
            csvColumns.values().stream()
                .filter(columnsToImport::contains)
                .collect(Collectors.joining(COMMA)),
            String.join(COMMA, values));
        try {
            statement.addBatch(cql);
        } catch (final SQLException e) {
            LOG.warn("Failed to add statement to the batch: {}", cql, e);
        }
    }

    private String handleNumberValue(final String strValue) {
        if (strValue == null) {
            return null;
        }
        try {
            final Number number = this.decimalFormat.parse(strValue.trim());
            return number.toString();
        } catch (final ParseException e) {
            LOG.warn("Failed to parse and convert value: {}, the value will be ignored.", strValue);
        }
        return null;
    }

    private String parseValue(final String value, final Integer colType) {
        if (getOptionValueAsString(OPTION_NULLVAL, DEFAULT_NULL_FORMAT).equals(value)) {
            return null;
        }
        switch (colType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return String.valueOf(Boolean.parseBoolean(value));
            case Types.BIGINT:
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return handleNumberValue(value);
            case Types.DATE:
                // We assume here the date values respect the default date format supported by Cassandra CQL.
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                // We assume here the time values respect the default time format supported by Cassandra CQL.
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                // We assume here the date-time values respect the default date-time format supported by Cassandra CQL.
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.NVARCHAR:
            case Types.NCHAR:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.DATALINK:
                return wrap(value, SINGLE_QUOTE);
            case Types.OTHER:
            case Types.ARRAY:
                // We assume the format provided for other types (tuples, maps, arrays, ...) is correct and does not
                // require any transformation.
                return value;
            default:
                // Types JAVA_OBJECT, DISTINCT, STRUCT, REF, ROWID, SQLXML, REF_CURSOR are not supported.
                return null;
        }
    }
}
