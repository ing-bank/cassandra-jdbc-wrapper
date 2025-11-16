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
import com.ing.data.cassandra.jdbc.ColumnDefinitions;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.sql.SQLSyntaxErrorException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.ing.data.cassandra.jdbc.ColumnDefinitions.Definition.buildDefinitionInAnonymousTable;
import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.buildSpecialCommandResultSet;
import static com.ing.data.cassandra.jdbc.utils.ByteBufferUtil.bytes;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.UNSUPPORTED_COPY_OPTIONS;
import static com.ing.data.cassandra.jdbc.utils.WarningConstants.INVALID_OPTION_VALUE;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Executor abstraction for common parts of the special commands {@code COPY}.
 *
 * @see CopyFromCommandExecutor
 * @see CopyToCommandExecutor
 */
@Slf4j
public abstract class AbstractCopyCommandExecutor implements SpecialCommandExecutor {

    static final char DEFAULT_DECIMAL_SEPARATOR = '.';
    static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ssZ";
    static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";
    static final String DEFAULT_NULL_FORMAT = "null";
    static final char DEFAULT_QUOTE_CHAR = '"';
    static final char DEFAULT_DELIMITER_CHAR = ',';
    static final char DEFAULT_ESCAPE_CHAR = '\\';

    // Common supported options
    static final String OPTION_DECIMALSEP = "DECIMALSEP";
    static final String OPTION_DELIMITER = "DELIMITER";
    static final String OPTION_ESCAPE = "ESCAPE";
    static final String OPTION_HEADER = "HEADER";
    static final String OPTION_NULLVAL = "NULLVAL";
    static final String OPTION_QUOTE = "QUOTE";
    static final String OPTION_THOUSANDSSEP = "THOUSANDSSEP";
    static final Set<String> SUPPORTED_OPTIONS = initSupportedOptions();

    String tableName = null;
    String columns = null;
    Properties options = new Properties();
    DecimalFormat decimalFormat;
    String dateTimeFormat;
    String dateFormat;
    String timeFormat;

    private static Set<String> initSupportedOptions() {
        final Set<String> options = new HashSet<>();
        options.add(OPTION_DECIMALSEP);
        options.add(OPTION_DELIMITER);
        options.add(OPTION_ESCAPE);
        options.add(OPTION_HEADER);
        options.add(OPTION_NULLVAL);
        options.add(OPTION_QUOTE);
        options.add(OPTION_THOUSANDSSEP);
        return options;
    }

    void checkOptions() throws SQLSyntaxErrorException {
        // Remove the supported options from the set of options found in the command, if the result set is not empty,
        // this means there are unsupported options.
        final Set<String> invalidKeys = new HashSet<>(this.options.stringPropertyNames());
        invalidKeys.removeAll(SUPPORTED_OPTIONS);
        if (!invalidKeys.isEmpty()) {
            throw new SQLSyntaxErrorException(format(UNSUPPORTED_COPY_OPTIONS, invalidKeys));
        }
    }

    void configureFormatters() {
        this.dateTimeFormat = DEFAULT_DATETIME_FORMAT;
        this.dateFormat = DEFAULT_DATE_FORMAT;
        this.timeFormat = DEFAULT_TIME_FORMAT;

        final char thousandsSeparator = getOptionValueAsChar(OPTION_THOUSANDSSEP, Character.MIN_VALUE);
        final DecimalFormatSymbols decimalSymbols = new DecimalFormatSymbols(Locale.getDefault());
        decimalSymbols.setDecimalSeparator(getOptionValueAsChar(OPTION_DECIMALSEP, DEFAULT_DECIMAL_SEPARATOR));
        if (thousandsSeparator != Character.MIN_VALUE) {
            decimalSymbols.setGroupingSeparator(thousandsSeparator);
        }
        this.decimalFormat = (DecimalFormat) NumberFormat.getNumberInstance();
        this.decimalFormat.setGroupingUsed(thousandsSeparator != Character.MIN_VALUE);
        this.decimalFormat.setDecimalFormatSymbols(decimalSymbols);
    }

    @SuppressWarnings("SameParameterValue")
    String getOptionValueAsString(final String optionName, final String defaultValue) {
        final String optionValue = this.options.getProperty(optionName);
        return defaultIfBlank(optionValue, defaultValue);
    }

    char getOptionValueAsChar(final String optionName, final char defaultValue) {
        final String optionValue = this.options.getProperty(optionName);
        if (isNotEmpty(optionValue)) {
            return optionValue.charAt(0);
        }
        return defaultValue;
    }

    int getOptionValueAsInt(final String optionName, final int defaultValue) {
        final String optionValue = this.options.getProperty(optionName);
        if (optionValue != null) {
            try {
                return parseInt(optionValue);
            } catch (final NumberFormatException e) {
                log.warn(INVALID_OPTION_VALUE, optionName, optionValue, defaultValue);
            }
        }
        return defaultValue;
    }

    /**
     * Generates a result set for the {@code COPY} command.
     *
     * @param actionVerb      The action verb (imported from/exported to) used in the result set depending on the
     *                        executed command.
     * @param processedRows   The total number of imported or exported rows.
     * @param executedBatches The number of batches executed.
     * @param skippedRows     The total number of skipped rows. The information about the skipped rows is only
     *                        displayed if this number is greater or equal to 0.
     * @return The result for the {@code COPY} command.
     * @implNote The result set is a single row with a string value in a column {@code result} following the model of
     * the result representation described in the examples presented in the
     * <a href="https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/cqlshCopy.html#Examples">cqlsh
     * documentation</a>.
     */
    ResultSet buildCopyCommandResultSet(final String actionVerb,
                                        final long processedRows,
                                        final int executedBatches,
                                        final int skippedRows) {
        String skippedRowsInfo = EMPTY;
        if (skippedRows >= 0) {
            skippedRowsInfo = format(" (%d skipped)", skippedRows);
        }
        final String result = format("%d row(s) %s 1 file in %d batch(es)%s.",
            processedRows, actionVerb, executedBatches, skippedRowsInfo);
        final ByteBuffer resultAsBytes = bytes(result);
        return buildSpecialCommandResultSet(
            new ColumnDefinitions.Definition[]{
                buildDefinitionInAnonymousTable("result", TEXT)
            }, List.of(List.of(resultAsBytes))
        );
    }

}
