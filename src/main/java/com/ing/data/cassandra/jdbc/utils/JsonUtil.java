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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ing.data.cassandra.jdbc.json.CassandraBlobDeserializer;
import com.ing.data.cassandra.jdbc.json.CassandraBlobSerializer;
import com.ing.data.cassandra.jdbc.json.CassandraDateDeserializer;
import com.ing.data.cassandra.jdbc.json.CassandraDateTimeDeserializer;
import com.ing.data.cassandra.jdbc.json.CassandraTimeDeserializer;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Utility methods used for JSON-type handling.
 */
public final class JsonUtil {

    static ObjectMapper objectMapperInstance = null;

    private JsonUtil() {
        // Private constructor to hide the public one.
    }

    /**
     * Gets a pre-configured {@link ObjectMapper} for JSON support.
     *
     * @return A pre-configured {@link ObjectMapper} for JSON support.
     */
    public static ObjectMapper getObjectMapper() {
        if (objectMapperInstance != null) {
            return objectMapperInstance;
        } else {
            final ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            objectMapper.registerModule(new JavaTimeModule());
            final SimpleModule cassandraExtensionsModule = new SimpleModule();
            cassandraExtensionsModule.addDeserializer(ByteBuffer.class, new CassandraBlobDeserializer());
            cassandraExtensionsModule.addDeserializer(LocalDate.class, new CassandraDateDeserializer());
            cassandraExtensionsModule.addDeserializer(LocalTime.class, new CassandraTimeDeserializer());
            cassandraExtensionsModule.addDeserializer(OffsetDateTime.class, new CassandraDateTimeDeserializer());
            cassandraExtensionsModule.addSerializer(ByteBuffer.class, new CassandraBlobSerializer());
            objectMapper.registerModule(cassandraExtensionsModule);
            objectMapperInstance = objectMapper;
            return objectMapper;
        }
    }

}
