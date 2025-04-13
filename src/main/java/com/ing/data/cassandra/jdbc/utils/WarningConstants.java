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

package com.ing.data.cassandra.jdbc.utils;

import java.sql.Array;
import java.sql.ResultSet;
import java.util.Map;

/**
 * Warning messages strings shared across the JDBC wrapper classes.
 * <p>
 *     These messages can only be used in loggers and necessary placeholders ({@code {}}) are directly integrated to
 *     the message constants to support SLF4J loggers.
 * </p>
 */
public final class WarningConstants {

    /**
     * Warning message used when redacting sensitive values in a JDBC URL fails due to an invalid URL format.
     */
    public static final String URL_REDACTION_FAILED = "Try to redact an invalid URL, ignore it";

    /**
     * Warning message used when retrieving a value in the internal {@code jdbc-driver.properties} file fails.
     * This message is a template expecting the name of the not found property (example:
     * {@code LOG.warn(DRIVER_PROPERTY_NOT_FOUND, "unknownProperty")}).
     */
    public static final String DRIVER_PROPERTY_NOT_FOUND = "Unable to get JDBC driver property: {}.";

    /**
     * Warning message used when the specified configuration file cannot be found. This message is a template expecting
     * the name of the configuration file (example: {@code LOG.warn(CONFIGURATION_FILE_NOT_FOUND, "cassandra.conf")}).
     */
    public static final String CONFIGURATION_FILE_NOT_FOUND =
        "The configuration file {} cannot be found, it will be ignored.";

    /**
     * Warning message used when parsing a connection parameter fails. This message is a template expecting:
     * <ol>
     *     <li>the parameter name or description</li>
     *     <li>the invalid value of the parameter</li>
     *     <li>the alternative value used (or if the parameter is ignored)</li>
     * </ol>
     * (example: {@code LOG.warn(PARAMETER_PARSING_FAILED, "retry policy", e.getMessage(), "skipping")}).
     */
    public static final String PARAMETER_PARSING_FAILED = "Error occurred while parsing {}: {} / {}...";

    /**
     * Warning message used in any SQL exception thrown when trying to extract properties from a JDBC URL.
     */
    public static final String PROPERTIES_PARSING_FROM_URL_FAILED = "Failed to extract properties from the given URL.";

    /**
     * Warning message used when the given execution profile does not exist and cannot be applied. This message is a
     * template expecting the name of the invalid profile (example:
     * {@code LOG.warn(INVALID_PROFILE_NAME, "undefined_profile")}).
     */
    public static final String INVALID_PROFILE_NAME =
        "No execution profile named [{}], keep the current active profile.";

    /**
     * Warning message used when the fetch size parameter is invalid and cannot be parsed. This message is a template
     * expecting the value of the invalid fetch size as first placeholder and the fallback value as second placeholder
     * (example: {@code LOG.warn(INVALID_FETCH_SIZE_PARAMETER, "invalid_size", 5000)}).
     */
    public static final String INVALID_FETCH_SIZE_PARAMETER =
        "Invalid fetch size parameter: '{}'. The default fetch size ({}) will be used instead.";

    /**
     * Warning message used when trying to close a prepared statement, and it failed for some reason. This message is a
     * template expecting the error message of the failure cause (example:
     * {@code LOG.warn(PREPARED_STATEMENT_CLOSING_FAILED, e.getMessage())}).
     */
    public static final String PREPARED_STATEMENT_CLOSING_FAILED = "Unable to close the prepared statement: {}";

    /**
     * Warning message used when the given class when setting an array value in a statement is not a valid
     * implementation of {@link Array}. This message is a template expecting the name of the invalid class (example:
     * {@code LOG.warn(INVALID_ARRAY_IMPLEMENTATION, x.getClass().getName())}).
     */
    public static final String INVALID_ARRAY_IMPLEMENTATION =
        "Unsupported SQL Array implementation: {}, an empty list will be inserted.";

    /**
     * Warning message used when getting the value of a column of type {@code list} from a {@link ResultSet} fails.
     * This generally happens when the class of items cannot be determined/found.
     */
    public static final String GET_LIST_FAILED = "Error while executing getList()";

    /**
     * Warning message used when getting the value of a column of type {@code set} from a {@link ResultSet} fails.
     * This generally happens when the class of items cannot be determined/found.
     */
    public static final String GET_SET_FAILED = "Error while executing getSet()";

    /**
     * Warning message used when getting the value of a column of type {@code vector} from a {@link ResultSet} fails.
     * This generally happens when the class of items cannot be determined/found.
     */
    public static final String GET_VECTOR_FAILED = "Error while executing getVector()";

    /**
     * Warning message used when the value of an option in a special CQL command is invalid. This message is a template
     * expecting the name of the option as first placeholder, the invalid value as second placeholder, and the default
     * value used instead as last placeholder (example:
     * {@code LOG.warn(INVALID_OPTION_VALUE, "optionName", "abc", "100")}).
     */
    public static final String INVALID_OPTION_VALUE =
        "Invalid value for option {}: {}. Will use the default value: {}.";

    /**
     * Warning message used when parsing a value in a CSV file used in the command {@code COPY FROM} fails. This
     * message is a template expecting the invalid value (example: {@code LOG.warn(PARSING_VALUE_FAILED, "abc")}).
     */
    public static final String PARSING_VALUE_FAILED =
        "Failed to parse and convert value: {}, the value will be ignored.";

    /**
     * Warning message used when counting the number of rows exported by the command {@code COPY TO} fails.
     */
    public static final String COUNTING_EXPORTED_ROWS_FAILED =
        "Failed to read exported CSV file to count exportedRows.";

    /**
     * Warning message used when adding a statement to a batch fails. This message is a template expecting the CQL
     * statement (example: {@code LOG.warn(ADD_STATEMENT_TO_BATCH_FAILED, "INSERT INTO table VALUES (1, 'a')")}).
     */
    public static final String ADD_STATEMENT_TO_BATCH_FAILED = "Failed to add statement to the batch: {}";

    /**
     * Warning message used when retrieving the JDBC type corresponding to a CQL type fails. This message is a template
     * expecting the CQL type as first placeholder and the error message as second placeholder (example:
     * {@code LOG.warn(JDBC_TYPE_NOT_FOUND_FOR_CQL_TYPE, "unknown", e.getMessage())}).
     */
    public static final String JDBC_TYPE_NOT_FOUND_FOR_CQL_TYPE = "Unable to get JDBC type for comparator [{}]: {}";

    /**
     * Warning message used when casting a value in a target type fails (typically when retrieving data in a given row).
     * This message is a template expecting:
     * <ol>
     *     <li>the value to cast</li>
     *     <li>the index of the value in the row</li>
     *     <li>the target type</li>
     *     <li>the returned value, by default</li>
     * </ol>
     * (example: {@code LOG.warn(INVALID_CAST, "abc", 1, "integer", 0)}).
     */
    public static final String INVALID_CAST = "Unable to cast [{}] (index {}) as {}, it will return {}.";

    /**
     * Warning message used when retrieving the cluster name fails. This message is a template
     * expecting the message of the cause (example: {@code LOG.warn(CLUSTER_NAME_NOT_FOUND, e.getMessage())}).
     */
    public static final String CLUSTER_NAME_NOT_FOUND = "Unable to retrieve the cluster name: {}";

    /**
     * Warning message used when retrieving the schema name fails. This message is a template
     * expecting the message of the cause (example: {@code LOG.warn(SCHEMA_NAME_NOT_FOUND, e.getMessage())}).
     */
    public static final String SCHEMA_NAME_NOT_FOUND = "Unable to retrieve the schema name: {}";

    /**
     * Warning message used when representing a {@link Map} as a string fails. This message is a template
     * expecting the basic string representation of the map value (example:
     * {@code LOG.warn(MAP_TO_STRING_FORMATTING_FAILED, map.toString())}).
     */
    public static final String MAP_TO_STRING_FORMATTING_FAILED = "Unable to format map [{}] as string";

    /**
     * Warning message used when setting a custom endpoint for Amazon Secrets manager fails.
     */
    public static final String INVALID_AWS_SECRETS_MANAGER_CUSTOM_ENDPOINT =
        "Unable to set a custom endpoint for Amazon Secrets manager, will fallback to the default endpoint.";

    /**
     * Warning message used when converting of an object to a byte array fails. This message is a template
     * expecting the class name of the converted object (example:
     * {@code LOG.warn(BINARY_FAILED_CONVERSION, o.getClass().getName())}).
     */
    public static final String BINARY_FAILED_CONVERSION = "Unable to convert {} object to byte array.";

    private WarningConstants() {
        // Private constructor to hide the public one.
    }

}
