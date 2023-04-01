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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.util.ClassUtil;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Deserializer for {@link OffsetDateTime}s in the context of a JSON returned by a CQL query.
 * <p>
 *     This deserializer expects a string using the format {@code yyyy-MM-dd HH:mm:ss.SSSZ} representing value with CQL
 *     type {@code timestamp}.
 * </p>
 */
public class CassandraDateTimeDeserializer extends JsonDeserializer<OffsetDateTime> {

    @Override
    public OffsetDateTime deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext)
        throws IOException {
        String value = jsonParser.getValueAsString();
        if (value != null) {
            // Ensure the offset value is valid and can be parsed to an OffsetDateTime.
            value = value.replace("Z", "+0000");
            try {
                final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss.SSSZ");
                return OffsetDateTime.parse(value, dateTimeFormatter);
            } catch (final DateTimeException e) {
                final String msg = String.format("Cannot deserialize value of type %s from: '%s'",
                    ClassUtil.nameOf(OffsetDateTime.class), value);
                throw InvalidFormatException.from(jsonParser, msg, value, OffsetDateTime.class);
            }
        } else {
            return null;
        }
    }

}
