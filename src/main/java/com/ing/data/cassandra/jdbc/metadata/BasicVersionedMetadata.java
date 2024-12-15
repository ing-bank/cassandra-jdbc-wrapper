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

import com.ing.data.cassandra.jdbc.CassandraConnection;
import org.semver4j.Semver;

import java.util.function.Function;

/**
 * A basic implementation of a versioned database metadata.
 *
 * @see VersionedMetadata
 * @see BuiltInFunctionsMetadataBuilder
 */
public class BasicVersionedMetadata implements VersionedMetadata {

    private final String metadataName;
    private final Semver validFrom;
    private final Semver invalidFrom;
    private final Function<CassandraConnection, Boolean> additionalCondition;

    /**
     * Constructs a database metadata valid in the specified range of Cassandra versions and verifying a specific
     * condition for the current connection.
     *
     * @param metadataName          The metadata name (a built-in function name for example).
     * @param validFrom             The minimal Cassandra version from which the metadata exists. If {@code null}, we
     *                              consider the metadata exists in any version of the Cassandra database.
     * @param invalidFrom           The first Cassandra version in which the metadata does not exist anymore. If
     *                              {@code null}, we consider the metadata exists in any version of the Cassandra
     *                              database greater than {@code validFrom}.
     * @param additionalCondition   An additional condition to verify on the current connection to the database.
     */
    public BasicVersionedMetadata(final String metadataName, final String validFrom, final String invalidFrom,
                                  final Function<CassandraConnection, Boolean> additionalCondition) {
        this.metadataName = metadataName;
        this.validFrom = Semver.coerce(validFrom);
        this.invalidFrom = Semver.coerce(invalidFrom);
        this.additionalCondition = additionalCondition;
    }

    /**
     * Constructs a database metadata valid in the specified range of Cassandra versions.
     *
     * @param metadataName The metadata name (a built-in function name for example).
     * @param validFrom    The minimal Cassandra version from which the metadata exists. If {@code null}, we consider
     *                     the metadata exists in any version of the Cassandra database.
     * @param invalidFrom  The first Cassandra version in which the metadata does not exist anymore. If {@code null},
     *                     we consider the metadata exists in any version of the Cassandra database greater than
     *                     {@code validFrom}.
     */
    public BasicVersionedMetadata(final String metadataName, final String validFrom, final String invalidFrom) {
        this(metadataName, validFrom, invalidFrom, connection -> true);
    }

    /**
     * Constructs a database metadata valid from the specified version of Cassandra and verifying a specific condition
     * for the current connection.
     *
     * @param metadataName          The metadata name (a built-in function name for example).
     * @param validFrom             The minimal Cassandra version from which the metadata exists.
     * @param additionalCondition   An additional condition to verify on the current connection to the database.
     */
    public BasicVersionedMetadata(final String metadataName, final String validFrom,
                                  final Function<CassandraConnection, Boolean> additionalCondition) {
        this(metadataName, validFrom, null, additionalCondition);
    }

    /**
     * Constructs a database metadata valid from the specified version of Cassandra.
     *
     * @param metadataName The metadata name (a built-in function name for example).
     * @param validFrom    The minimal Cassandra version from which the metadata exists.
     */
    public BasicVersionedMetadata(final String metadataName, final String validFrom) {
        this(metadataName, validFrom, (String) null);
    }

    /**
     * Constructs a database metadata valid from the specified version of Cassandra and verifying a specific condition
     * for the current connection.
     *
     * @param metadataName          The metadata name (a built-in function name for example).
     * @param additionalCondition   An additional condition to verify on the current connection to the database.
     */
    public BasicVersionedMetadata(final String metadataName,
                                  final Function<CassandraConnection, Boolean> additionalCondition) {
        this(metadataName, null, null, additionalCondition);
    }

    /**
     * Constructs a database metadata valid in any version of Cassandra.
     *
     * @param metadataName The metadata name (a built-in function name for example).
     */
    public BasicVersionedMetadata(final String metadataName) {
        this(metadataName, (String) null);
    }

    @Override
    public String getName() {
        return this.metadataName;
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
}
