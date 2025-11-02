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

import jakarta.annotation.Nullable;

/**
 * A versioned database metadata (such as a CQL type, CQL keyword or a built-in function).
 */
public interface VersionedMetadata {

    /**
     * Gets the metadata name (for example a CQL keyword, or a built-in function name).
     *
     * @return The metadata name.
     */
    String getName();

    /**
     * Gets the minimal Cassandra version from which the metadata exists. If {@code null}, we consider the metadata
     * always existed.
     *
     * @return The minimal version of Cassandra from which the metadata exists or {@code null}.
     */
    Semver isValidFrom();

    /**
     * Gets the first Cassandra version in which the metadata does not exist anymore. If {@code null}, it means the
     * metadata still exists in the latest version of Cassandra.
     *
     * @return The first version of Cassandra in which the metadata does not exist anymore or {@code null}.
     */
    Semver isInvalidFrom();

    /**
     * Checks if the connection to the database fulfills some specific conditions defined in the implementation of
     * {@code VersionedMetadata}.
     *
     * @param connection The connection to the database.
     * @return {@code true} if the additional condition is verified by the connection, {@code false} otherwise.
     * @implSpec Should return {@code true} if the connection is {@code null}.
     */
    boolean fulfillAdditionalCondition(@Nullable CassandraConnection connection);

}
