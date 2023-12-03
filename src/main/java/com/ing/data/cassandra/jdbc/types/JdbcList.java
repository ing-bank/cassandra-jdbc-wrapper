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

import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;

import java.util.List;

/**
 * JDBC description of {@code LIST} CQL type (corresponding Java type: {@link List}).
 * <p>CQL type description: a collection of one or more ordered elements.</p>
 */
@SuppressWarnings("rawtypes")
public class JdbcList extends AbstractJdbcCollection<List> {

    /**
     * Gets a {@code JdbcList} instance.
     */
    public static final JdbcList INSTANCE = new JdbcList();

    JdbcList() {
    }

    @Override
    public String toString(final List obj) {
        return Iterables.toString(obj);
    }

    @Override
    public Class<List> getType() {
        return List.class;
    }

    @Override
    public List compose(final Object obj) {
        if (obj != null && obj.getClass().isAssignableFrom(List.class)) {
            return (List) obj;
        }
        return null;
    }

    @Override
    public Object decompose(final List value) {
        return value;
    }

}
