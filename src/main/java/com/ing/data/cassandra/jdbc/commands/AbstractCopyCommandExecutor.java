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
import com.ing.data.cassandra.jdbc.CassandraStatement;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.LOG;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.UNSUPPORTED_COPY_OPTIONS;

/**
 * Executor abstraction for common parts of the special commands {@code COPY}.
 *
 * @see CopyFromCommandExecutor
 * @see CopyToCommandExecutor
 */
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
    static final Set<String> SUPPORTED_OPTIONS = new HashSet<String>() {
        {
            add(OPTION_DECIMALSEP);
            add(OPTION_DELIMITER);
            add(OPTION_ESCAPE);
            add(OPTION_HEADER);
            add(OPTION_NULLVAL);
            add(OPTION_QUOTE);
            add(OPTION_THOUSANDSSEP);
        }
    };

    String tableName = null;
    String columns = null;
    Properties options = new Properties();
    DecimalFormat decimalFormat;
    String dateTimeFormat;
    String dateFormat;
    String timeFormat;

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract ResultSet execute(CassandraStatement statement, String cql) throws SQLException;

    void checkOptions() throws SQLSyntaxErrorException {
        // Remove the supported options from the set of options found in the command, if the result set is not empty,
        // this means there are unsupported options.
        final Set<String> invalidKeys = new HashSet<>(this.options.stringPropertyNames());
        invalidKeys.removeAll(SUPPORTED_OPTIONS);
        if (!invalidKeys.isEmpty()) {
            throw new SQLSyntaxErrorException(String.format(UNSUPPORTED_COPY_OPTIONS, invalidKeys));
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

    String getOptionValueAsString(final String optionName, final String defaultValue) {
        final String optionValue = this.options.getProperty(optionName);
        return StringUtils.defaultIfBlank(optionValue, defaultValue);
    }

    char getOptionValueAsChar(final String optionName, final char defaultValue) {
        final String optionValue = this.options.getProperty(optionName);
        if (StringUtils.isNotEmpty(optionValue)) {
            return optionValue.charAt(0);
        }
        return defaultValue;
    }

    int getOptionValueAsInt(final String optionName, final int defaultValue) {
        final String optionValue = this.options.getProperty(optionName);
        if (optionValue != null) {
            try {
                return Integer.parseInt(optionValue);
            } catch (final NumberFormatException e) {
                LOG.warn("Invalid value for option {}: {}. Will use the default value: {}.",
                    optionName, optionValue, defaultValue);
            }
        }
        return defaultValue;
    }

}
