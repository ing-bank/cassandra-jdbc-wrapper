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

package com.ing.data.cassandra.jdbc.types;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.ing.data.cassandra.jdbc.utils.WarningConstants.MAP_TO_STRING_FORMATTING_FAILED;

/**
 * JDBC description of {@code MAP} CQL type (corresponding Java type: {@link Map}).
 * <p>CQL type description: a JSON-style array of literals.</p>
 */
@SuppressWarnings("rawtypes")
public class JdbcMap extends AbstractJdbcCollection<Map> {

    /**
     * Gets a {@code JdbcMap} instance.
     */
    public static final JdbcMap INSTANCE = new JdbcMap();

    private static final Logger LOG = LoggerFactory.getLogger(JdbcMap.class);

    JdbcMap() {
    }

    @Override
    public String toString(final Map obj) {
        try {
            return new ObjectMapper().writeValueAsString(obj);
        } catch (final JsonProcessingException e) {
            LOG.warn(MAP_TO_STRING_FORMATTING_FAILED, obj.toString(), e);
            return null;
        }
    }

    @Override
    public Class<Map> getType() {
        return Map.class;
    }

    @Override
    public Map compose(final Object obj) {
        if (obj != null && obj.getClass().isAssignableFrom(Map.class)) {
            return (Map) obj;
        }
        return null;
    }

    @Override
    public Object decompose(final Map value) {
        return value;
    }

}
