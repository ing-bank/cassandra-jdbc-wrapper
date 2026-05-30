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

package com.ing.data.cassandra.jdbc.json;

import tools.jackson.databind.ext.javatime.deser.LocalTimeDeserializer;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Deserializer for {@link LocalTime}s in the context of a JSON returned by a CQL query.
 * <p>
 *     This deserializer expects a string using the format {@code HH:mm:ss.SSSSSSSSS} representing value with CQL type
 *     {@code time}.
 * </p>
 */
public class CassandraTimeDeserializer extends LocalTimeDeserializer {

    /**
     * Constructor.
     * <p>
     *     The values with CQL type {@code time} returned by Cassandra in generated JSON use the format
     *     {@code HH:mm:ss.SSSSSSSSS}.
     * </p>
     */
    public CassandraTimeDeserializer() {
        super(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS"));
    }

}
