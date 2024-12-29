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

import com.ing.data.cassandra.jdbc.CassandraPreparedStatement;
import com.ing.data.cassandra.jdbc.CassandraResultSet;
import com.ing.data.cassandra.jdbc.CassandraStatement;
import com.ing.data.cassandra.jdbc.metadata.MetadataRow;

import java.net.URI;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Error messages strings shared across the JDBC wrapper classes.
 */
public final class ErrorConstants {

    /**
     * Error message used in any SQL exception thrown when a method is called on a closed {@link Connection}.
     */
    public static final String WAS_CLOSED_CONN = "Method was called on a closed Connection.";

    /**
     * Error message used in any SQL exception thrown when a method is called on a closed {@link Statement}.
     */
    public static final String WAS_CLOSED_STMT = "Method was called on a closed Statement.";

    /**
     * Error message used in any SQL exception thrown when a method is called on a closed {@link ResultSet}.
     */
    public static final String WAS_CLOSED_RS = "Method was called on a closed ResultSet.";

    /**
     * Error message used in any SQL exception thrown when the method {@code unwrap(Class)} is called with a class
     * not matching the expected interface. This message is a template expecting the name of the class parameter as
     * placeholder (example: {@code String.format(NO_INTERFACE, iface.getSimpleName())}).
     */
    public static final String NO_INTERFACE = "No object was found that matched the provided interface: %s";

    /**
     * Error message used in any SQL exception thrown because the called method requires transactions (currently not
     * implemented in Cassandra).
     */
    public static final String NO_TRANSACTIONS = "The Cassandra implementation does not support transactions.";

    /**
     * Error message used in any SQL exception thrown because the called method requires a non-committed transaction
     * (but transactions are not currently implemented in Cassandra, so we consider we are always in auto-commit mode).
     */
    public static final String ALWAYS_AUTOCOMMIT = "The Cassandra implementation is always in auto-commit mode.";

    /**
     * Error message used in any SQL exception thrown because the provided timeout is invalid (less than 0).
     */
    public static final String BAD_TIMEOUT = "The timeout value was less than zero.";

    /**
     * Error message used in any SQL exception thrown because the called method requires a feature not currently
     * supported by Cassandra.
     */
    public static final String NOT_SUPPORTED = "The Cassandra implementation does not support this method.";

    /**
     * Error message used in any SQL exception thrown because the called method requires auto-generated keys (currently
     * not implemented in Cassandra).
     */
    public static final String NO_GEN_KEYS =
        "The Cassandra implementation does not currently support returning generated keys.";

    /**
     * Error message used in any SQL exception thrown because the called method requires keeping multiple open result
     * sets (currently not implemented in Cassandra).
     */
    public static final String NO_MULTIPLE =
        "The Cassandra implementation does not currently support multiple open result sets.";

    /**
     * Error message used in any SQL exception thrown because a {@code null} result set has been returned by the
     * Java Driver for Apache Cassandra® when a query is executed.
     */
    public static final String NO_RESULT_SET =
        "No ResultSet returned from the CQL statement passed in an 'executeQuery()' method.";

    /**
     * Error message used in any SQL exception thrown when the parameter passed to the method
     * {@code Statement.getMoreResults(int)} is invalid. This message is a template expecting the value of the
     * invalid parameter as placeholder (example: {@code String.format(BAD_KEEP_RS, 9)}).
     */
    public static final String BAD_KEEP_RS =
        "The argument for keeping the current result set: %d is not a valid value.";

    /**
     * Error message used in any SQL exception thrown when the expected type of result set for a
     * {@link Statement} is invalid. This message is a template expecting the value of the
     * invalid type of result set as placeholder (example: {@code String.format(BAD_TYPE_RS, 1099)}).
     */
    public static final String BAD_TYPE_RS = "The argument for result set type: %d is not a valid value.";

    /**
     * Error message used in any SQL exception thrown when the expected result set concurrency for a
     * {@link Statement} is invalid. This message is a template expecting the value of the
     * invalid result set concurrency value as placeholder (example: {@code String.format(BAD_CONCURRENCY_RS, 1099)}).
     */
    public static final String BAD_CONCURRENCY_RS =
        "The argument for result set concurrency: %d is not a valid value.";

    /**
     * Error message used in any SQL exception thrown when the expected result set holdability for a
     * {@link Statement} is invalid. This message is a template expecting the value of the
     * invalid result set holdability value as placeholder (example: {@code String.format(BAD_HOLD_RS, 9)}).
     */
    public static final String BAD_HOLD_RS =
        "The argument for result set holdability: %d is not a valid value.";

    /**
     * Error message used in any SQL exception thrown when the expected fetching direction for a
     * {@link Statement} or a {@link ResultSet} is invalid. This message is a template expecting the
     * value of the invalid fetching direction value as placeholder (example:
     * {@code String.format(BAD_FETCH_DIR, 1099)}).
     */
    public static final String BAD_FETCH_DIR = "Fetch direction value of: %d is illegal.";

    /**
     * Error message used in any SQL exception thrown when the expected key auto-generation parameter used in a
     * {@link Statement} is invalid. Note that auto-generated keys are currently not implemented in Cassandra.
     * This message is a template expecting the value of the invalid parameter as placeholder (example:
     * {@code String.format(BAD_AUTO_GEN, 9)}).
     */
    public static final String BAD_AUTO_GEN = "Auto key generation value of: %d is illegal.";

    /**
     * Error message used in any SQL exception thrown when the specified fetch size for a
     * {@link Statement} or a {@link ResultSet} is negative. This message is a template expecting the
     * value of the invalid fetch size value as placeholder (example: {@code String.format(BAD_FETCH_SIZE, -10)}).
     */
    public static final String BAD_FETCH_SIZE = "Fetch size of: %d rows may not be negative.";

    /**
     * Error message used when the fetch size parameter is invalid and cannot be parsed. This message is a template
     * expecting the value of the invalid fetch size as first placeholder and the fallback value as second placeholder
     * (example: {@code String.format(INVALID_FETCH_SIZE_PARAMETER, "invalid_size", 5000)}).
     */
    public static final String INVALID_FETCH_SIZE_PARAMETER =
        "Invalid fetch size parameter: '%s'. The default fetch size (%d) will be used instead.";

    /**
     * Error message used in any SQL exception thrown when the specified column index in a {@link ResultSet}
     * is not strictly positive or greater than the number of columns in the result set. This message is a template
     * expecting the value of the invalid index value as placeholder (example:
     * {@code String.format(MUST_BE_POSITIVE, 0)}).
     */
    public static final String MUST_BE_POSITIVE =
        "Index must be a positive number less or equal the count of returned columns: %d";

    /**
     * Error message used in any SQL exception thrown when the specified index for a variable binding in a
     * {@link PreparedStatement} is greater than the number of binding variable markers in the CQL query. This message
     * is a template expecting the value of the invalid index and the number of markers as placeholders (example:
     * {@code String.format(OUT_OF_BOUNDS_BINDING_INDEX, 5, 3)}).
     */
    public static final String OUT_OF_BOUNDS_BINDING_INDEX =
        "The index %d is greater than the count of bound variable markers in the CQL: %d";

    /**
     * Error message used in any SQL exception thrown when the specified index for a variable binding in a
     * {@link PreparedStatement} is not strictly positive. This message is a template expecting the value of the
     * invalid index value as placeholder (example: {@code String.format(MUST_BE_POSITIVE_BINDING_INDEX, 0)}).
     */
    public static final String MUST_BE_POSITIVE_BINDING_INDEX = "The binding index must be a positive number: %d";

    /**
     * Error message used in any exception thrown when the specified column name in a {@link ResultSet} or a row
     * definition is invalid. This message is a template expecting the value of the invalid column name as placeholder
     * (example: {@code String.format(VALID_LABELS, "invalid_column")}).
     */
    public static final String VALID_LABELS = "Name provided was not in the list of valid column labels: %s";

    /**
     * Error message used in any SQL exception thrown when the JDBC URL does not specify any host name.
     */
    public static final String HOST_IN_URL =
        "Connection url must specify a host, e.g. jdbc:cassandra://localhost:9042/keyspace";

    /**
     * Error message used in any SQL exception thrown when the contact points in the JDBC URL cannot be parsed. This
     * message is a template expecting the value of the invalid contact point as placeholder (example:
     * {@code String.format(INVALID_CONTACT_POINT, "invalid:host")}).
     */
    public static final String INVALID_CONTACT_POINT = "Invalid contact point: %s";

    /**
     * Error message used in any SQL exception thrown when a connection cannot be established due to a missing host
     * name.
     */
    public static final String HOST_REQUIRED = "A host name is required to build a connection.";

    /**
     * Error message used in any SQL exception thrown when the specified keyspace name is invalid. This message is a
     * template expecting the value of the invalid keyspace name as placeholder (example:
     * {@code String.format(BAD_KEYSPACE, "invalid_key$pace")}).
     */
    public static final String BAD_KEYSPACE =
        "Keyspace names must be composed of alphanumerics and underscores (parsed: '%s').";

    /**
     * Error message used in any SQL exception thrown when the provided JDBC URL contains not allowed user information
     * (see {@link URI#getUserInfo()}).
     */
    public static final String URI_IS_SIMPLE =
        "Connection URL may only include host, port, keyspace, and allowed options, e.g. "
            + "jdbc:cassandra://localhost:9042/keyspace?consistency=ONE";

    /**
     * Error message used in any SQL exception thrown when the required parameter {@code secureconnectbundle} is
     * missing in the JDBC URL.
     */
    public static final String SECURECONENCTBUNDLE_REQUIRED = "A 'secureconnectbundle' parameter is required.";

    /**
     * Error message used in any SQL exception thrown when the required parameter {@code awsregion} is missing in the
     * JDBC URL.
     */
    public static final String AWS_REGION_REQUIRED = "The 'awsregion' parameter is required.";

    /**
     * Error message used in any SQL exception thrown when either the required parameter {@code awsregion} or
     * {@code awssecretregion} is missing in the JDBC URL.
     */
    public static final String AWS_REGION_FOR_SECRET_REQUIRED =
        "Either 'awsregion' or 'awssecretregion' parameter is required.";

    /**
     * Error message used in any SQL exception thrown when retrieving of a secret value in AWS Secret Manager failed.
     * This message is a template expecting the name of the secret and AWS region and the error message returned by the
     * AWS Secret Manager (example:
     * {@code String.format(AWS_SECRET_RETRIEVAL_FAILED, "mySecret", "eu-west-1", e.awsErrorDetails().errorMessage())}).
     */
    public static final String AWS_SECRET_RETRIEVAL_FAILED =
        "Unable to retrieve the secret [%s] in the region [%s]: %s";

    /**
     * Error message used in any SQL exception thrown because the {@link ResultSet} type is set to
     * {@link ResultSet#TYPE_FORWARD_ONLY} (but cursors are currently not implemented in Cassandra).
     */
    public static final String FORWARD_ONLY = "Can not position cursor with a type of TYPE_FORWARD_ONLY.";

    /**
     * Error message used in any SQL exception thrown when the method {@link ResultSet#getURL(int)} or
     * {@link ResultSet#getURL(String)} is invoked on a column containing an invalid URL. This message is a template
     * expecting the invalid value as placeholder (example: {@code String.format(MALFORMED_URL, "not_a_valid_url")}).
     */
    public static final String MALFORMED_URL = "The string '%s' is not a valid URL.";

    /**
     * Error message used in any SQL exception thrown when the SSL configuration for the connection fails. This message
     * is a template expecting the message of the error cause as placeholder (example:
     * {@code String.format(SSL_CONFIG_FAILED, "Invalid certificate")}).
     */
    public static final String SSL_CONFIG_FAILED = "Unable to configure SSL: %s.";

    /**
     * Error message used in any SQL exception thrown when the method {@link CassandraResultSet#getVector(int)} or
     * {@link CassandraResultSet#getVector(String)} is invoked on a column containing an invalid CQL vector.
     */
    public static final String VECTOR_ELEMENTS_NOT_NUMBERS = "Vector elements are not numbers.";

    /**
     * Error message used in any SQL exception thrown when the target JDBC type specified in the method
     * {@link CassandraPreparedStatement#setObject(int, Object, int)} and its variants is not supported.
     */
    public static final String UNSUPPORTED_JDBC_TYPE = "Unsupported JDBC type: %s";

    /**
     * Error message used in any SQL exception thrown when the conversion of the specified object in the method
     * {@link CassandraPreparedStatement#setObject(int, Object, int)} and its variants is not supported.
     */
    public static final String UNSUPPORTED_PARAMETER_TYPE = "Unsupported parameter type: %s";

    /**
     * Error message used in any SQL exception thrown when the conversion to the specified type in the methods
     * {@link CassandraResultSet#getObject(int, Class)} and {@link CassandraResultSet#getObject(String, Class)} is not
     * supported.
     */
    public static final String UNSUPPORTED_TYPE_CONVERSION = "Conversion to type %s not supported.";

    /**
     * Error message used in any SQL exception thrown when the conversion to a specific type in a getter method of
     * {@link CassandraResultSet} failed.
     */
    public static final String UNABLE_TO_READ_VALUE = "Unable to read value as %s.";

    /**
     * Error message used in any SQL exception thrown when the conversion to the specified type in the methods
     * {@link CassandraResultSet#getObjectFromJson(int, Class)},
     * {@link CassandraResultSet#getObjectFromJson(String, Class)} and
     * {@link CassandraResultSet#getObjectFromJson(Class)} is not supported.
     */
    public static final String UNSUPPORTED_JSON_TYPE_CONVERSION =
        "Unable to convert the column of index %d to an instance of %s";

    /**
     * Error message used in any SQL exception thrown when the conversion to JSON for the specified object in the method
     * {@link CassandraPreparedStatement#setJson(int, Object)} is not supported.
     */
    public static final String UNSUPPORTED_CONVERSION_TO_JSON =
        "Unable to convert the object of type %s to bind the column of index %d";

    /**
     * Error message used in any SQL exception thrown when it is not possible to retrieve some metadata of any
     * {@link ResultSet}.
     */
    public static final String UNABLE_TO_RETRIEVE_METADATA = "Unable to retrieve metadata for result set.";

    /**
     * Error message used in any runtime exception thrown when populating a {@link MetadataRow} failed due to a mismatch
     * between the number of provided values and the number of columns in the row.
     */
    public static final String UNABLE_TO_POPULATE_METADATA_ROW = "Unable to populate a metadata row.";

    /**
     * Error message used in any SQL exception thrown when the number of CQL queries included in a single batch of
     * statements is greater than the allowed limit ({@value CassandraStatement#MAX_ASYNC_QUERIES} for a prepared batch
     * statement or 1.1 {@code *} {@value CassandraStatement#MAX_ASYNC_QUERIES} for a split single statement).
     * This message is a template expecting the number of CQL queries to execute as placeholder (example:
     * {@code String.format(TOO_MANY_QUERIES, 10000)}).
     */
    public static final String TOO_MANY_QUERIES =
        "Too many queries at once (%d). You must split your queries into more batches!";

    /**
     * Error message used in any SQL exception thrown when the fetch direction specified on a ResultSet is not
     * supported for the type {@code TYPE_FORWARD_ONLY}. This message is a template expecting the illegal fetch
     * direction as placeholder (example:
     * {@code String.format(ILLEGAL_FETCH_DIRECTION_FOR_FORWARD_ONLY, FETCH_UNKNOWN)}).
     */
    public static final String ILLEGAL_FETCH_DIRECTION_FOR_FORWARD_ONLY =
        "Attempt to set an illegal fetch direction for TYPE_FORWARD_ONLY: %d";

    /**
     * Error message used in any SQL exception thrown when retrieving metadata related to a catalog and the given one
     * is not {@code null} or does not match the one of the current connection.
     */
    public static final String INVALID_CATALOG_NAME = "Catalog name must exactly match or be null.";

    /**
     * Error message used in any SQL exception thrown when the creation of the connection to the database fails for
     * any reason. The underlying exception should be logged in this case.
     */
    public static final String CONNECTION_CREATION_FAILED = "Unexpected error while creating connection.";

    /**
     * Error message used in any SQL exception thrown when trying to access to an {@link Array} object previously
     * freed.
     */
    public static final String ARRAY_WAS_FREED = "Array was freed.";

    /**
     * Error message used in any batch update exception thrown when at least one statement fails or attempts to return
     * a result set in the method {@link Statement#executeBatch()}. This message should be followed by the details for
     * each failed statement (example: {@code BATCH_UPDATE_FAILED + "Statement failed"}).
     *
     * @see #BATCH_STATEMENT_FAILURE_MSG
     */
    public static final String BATCH_UPDATE_FAILED = "At least one statement in batch has failed:";

    /**
     * Template used to report a statement failure or attempting to return a result set in the method
     * {@link Statement#executeBatch()}. This template should be used in {@link #BATCH_UPDATE_FAILED} and expects
     * the statement index in the batch and the error message (example:
     * {@code String.format(BATCH_STATEMENT_FAILURE_MSG, 1, "Statement failed"}).
     *
     * @see #BATCH_UPDATE_FAILED
     */
    public static final String BATCH_STATEMENT_FAILURE_MSG = "\n - Statement #%d: %s";

    /**
     * Error message used when the given execution profile does not exist and cannot be applied. This message is a
     * template expecting the name of the invalid profile (example:
     * {@code String.format(INVALID_PROFILE_NAME, "undefined_profile")}).
     */
    public static final String INVALID_PROFILE_NAME =
        "No execution profile named [%s], keep the current active profile.";

    /**
     * Error message used in any SQL exception thrown when trying to extract properties from a JDBC URL.
     */
    public static final String PROPERTIES_PARSING_FROM_URL_FAILED = "Failed to extract properties from the given URL.";

    /**
     * Error message used in any SQL exception thrown when trying to execute the special CQL command {@code SOURCE}
     * without specifying any filename.
     */
    public static final String MISSING_SOURCE_FILENAME = "A filename must be specified for the command SOURCE.";

    /**
     * Error message used when the specified file in the special CQL command {@code SOURCE} cannot be open. This
     * message is a template expecting the name of the file and the error message (example:
     * {@code String.format(CANNOT_OPEN_SOURCE_FILE, "file.cql", "file does not exist")}).
     */
    public static final String CANNOT_OPEN_SOURCE_FILE = "Could not open '%s': %s";

    private ErrorConstants() {
        // Private constructor to hide the public one.
    }

}
