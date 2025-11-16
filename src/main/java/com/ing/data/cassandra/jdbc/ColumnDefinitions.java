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

package com.ing.data.cassandra.jdbc;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import jakarta.annotation.Nonnull;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.VALID_LABELS;

/**
 * Metadata describing the columns returned in a {@link CassandraResultSet} or a {@link CassandraPreparedStatement}.
 * <p>
 *     A {@code ColumnDefinitions} instance is mainly a list of {@link Definition}. The definitions or metadata for a
 *     column can be accessed either by:
 *     <ul>
 *         <li>index (indexed from 0)</li>
 *         <li>name</li>
 *     </ul>
 * </p>
 * <p>
 *     When accessed by name, column selection is case insensitive. In case multiple columns only differ by the case of
 *     their name, then the column returned will be the first column that has been defined in CQL without forcing case
 *     sensitivity (that is, it has either been defined without quotes or is fully lowercase). If none of the columns
 *     have been defined in this manner, the first column matching (with case insensitivity) is returned. You can force
 *     the case of a selection by using double quotes (") around the name.
 * </p>
 * <p>
 *     For example, considering {@code cd} an instance of {@code ColumnDefinitions}:
 *     <ul>
 *         <li>if {@code cd} contains column {@code bAR}, then {@code cd.contains("bar")}, {@code cd.contains("bAR")}
 *         and {@code cd.contains("Bar")} will return {@code true}</li>
 *         <li>if {@code cd} contains both {@code foo} and {@code FOO} then:
 *         <ul>
 *             <li>{@code cd.getType("foo")}, {@code cd.getType("fOO")} and {@code cd.getType("FOO")} will all match
 *             the column {@code foo}</li>
 *             <li>{@code cd.getType("\"FOO\"")} will match the column {@code FOO}</li>
 *         </ul>
 *     </ul>
 *     Note that the preceding rules mean that if a {@code ColumnDefinitions} object contains multiple occurrences of
 *     the exact same name (either the same column multiple times or columns from different tables with the same name),
 *     you will have to use selection by index to disambiguate.
 * </p>
 */
public class ColumnDefinitions implements Iterable<ColumnDefinitions.Definition> {

    private final Definition[] byIdx;
    private final Map<String, int[]> byName;

    /**
     * Constructor.
     *
     * @param definitions Array of columns definitions.
     */
    public ColumnDefinitions(final Definition[] definitions) {
        this.byIdx = definitions;
        this.byName = new HashMap<>(definitions.length);

        for (int i = 0; i < definitions.length; i++) {
            // Be optimistic, 99% of the time, previous will be null.
            final int[] previous = this.byName.put(definitions[i].name.toLowerCase(), new int[]{i});
            if (previous != null) {
                final int[] indexes = new int[previous.length + 1];
                System.arraycopy(previous, 0, indexes, 0, previous.length);
                indexes[indexes.length - 1] = i;
                this.byName.put(definitions[i].name.toLowerCase(), indexes);
            }
        }
    }

    /**
     * Returns the number of columns described by this {@code ColumnDefinitions} instance.
     *
     * @return The number of columns described by these metadata.
     */
    public int size() {
        return this.byIdx.length;
    }

    /**
     * Returns whether these metadata contains a given name.
     *
     * @param name The name to check.
     * @return {@code true} if these metadata contains the column named {@code name}, {@code false} otherwise.
     */
    public boolean contains(final String name) {
        return findAllIdx(name) != null;
    }

    /**
     * Returns the first index in these metadata of the provided column name, if present.
     *
     * @param name The name of the column.
     * @return The index of the first occurrence of the column name in these metadata if present, -1 otherwise.
     */
    public int getIndexOf(final String name) {
        return findFirstIdx(name);
    }

    /**
     * Returns an iterator over the {@link Definition} contained in these metadata.
     * <p>
     *     The order of the iterator will be the one of these metadata.
     * </p>
     *
     * @return An iterator over the {@link Definition} contained in these metadata.
     */
    @Override
    @Nonnull
    public Iterator<Definition> iterator() {
        return Arrays.asList(this.byIdx).iterator();
    }

    /**
     * Returns a list containing all the definitions of these metadata ordered by index.
     *
     * @return A list of the {@link Definition} contained in these metadata.
     */
    public List<Definition> asList() {
        return Arrays.asList(this.byIdx);
    }

    /**
     * Returns the name of the {@code i}th column in these metadata.
     *
     * @param i The index of the column in these metadata (the first column is 0).
     * @return The name of the {@code i}th column in these metadata.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public String getName(final int i) {
        return this.byIdx[i].name;
    }

    /**
     * Returns the type of the {@code i}th column in these metadata.
     *
     * @param i The index of the column in these metadata (the first column is 0).
     * @return The type of the {@code i}th column in these metadata.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public DataType getType(final int i) {
        return byIdx[i].type;
    }

    /**
     * Returns the type of the first occurrence of the column {@code name} in these metadata.
     *
     * @param name The name of the column.
     * @return The type of (the first occurrence of) the column {@code name} in these metadata.
     * @throws IllegalArgumentException if {@code name} is not in these metadata.
     */
    public DataType getType(final String name) {
        return getType(getFirstIdx(name));
    }

    /**
     * Returns the keyspace of the {@code i}th column in these metadata.
     *
     * @param i The index of the column in these metadata (the first column is 0).
     * @return The keyspace of the {@code i}th column in these metadata.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public String getKeyspace(final int i) {
        return this.byIdx[i].keyspace;
    }

    /**
     * Returns the keyspace of the first occurrence of the column {@code name} in these metadata.
     *
     * @param name The name of the column.
     * @return The keyspace of (the first occurrence of) the column {@code name} in these metadata.
     * @throws IllegalArgumentException if {@code name} is not in these metadata.
     */
    public String getKeyspace(final String name) {
        return getKeyspace(getFirstIdx(name));
    }

    /**
     * Returns the table name of the {@code i}th column in these metadata.
     *
     * @param i The index of the column in these metadata (the first column is 0).
     * @return The table name of the {@code i}th column in these metadata.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public String getTable(final int i) {
        return this.byIdx[i].table;
    }

    /**
     * Returns the table name of the first occurrence of the column {@code name} in these metadata.
     *
     * @param name The name of the column.
     * @return The table name of (the first occurrence of) the column {@code name} in these metadata.
     * @throws IllegalArgumentException if {@code name} is not in these metadata.
     */
    public String getTable(final String name) {
        return getTable(getFirstIdx(name));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Columns[");
        for (int i = 0; i < size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            final Definition def = byIdx[i];
            sb.append(def.name).append('(').append(def.type).append(')');
        }
        sb.append(']');
        return sb.toString();
    }

    int findFirstIdx(final String name) {
        final int[] indexes = findAllIdx(name);
        if (indexes == null) {
            return -1;
        } else {
            return indexes[0];
        }
    }

    int[] findAllIdx(final String name) {
        boolean caseSensitive = false;
        String columnName = name;
        if (name.length() >= 2 && name.charAt(0) == '"' && name.charAt(name.length() - 1) == '"') {
            columnName = name.substring(1, name.length() - 1);
            caseSensitive = true;
        }

        final int[] indexes = this.byName.get(columnName.toLowerCase());
        if (!caseSensitive || indexes == null) {
            return indexes;
        }

        // First, optimistic and assume all are matching.
        int nbMatch = 0;
        for (final int index : indexes) {
            if (columnName.equals(this.byIdx[index].name)) {
                nbMatch++;
            }
        }

        if (nbMatch == indexes.length) {
            return indexes;
        }

        final int[] result = new int[nbMatch];
        int j = 0;
        for (final int idx : indexes) {
            if (columnName.equals(this.byIdx[idx].name)) {
                result[j++] = idx;
            }
        }

        return result;
    }

    int[] getAllIdx(final String name) {
        final int[] indexes = findAllIdx(name);
        if (indexes == null) {
            throw new IllegalArgumentException(String.format(VALID_LABELS, name));
        }
        return indexes;
    }

    int getFirstIdx(final String name) {
        return getAllIdx(name)[0];
    }

    /**
     * A column definition.
     */
    @Getter
    public static class Definition {

        /**
         * The name of the keyspace this column is part of.
         */
        private final String keyspace;
        /**
         * The name of the table this column is part of.
         */
        private final String table;
        /**
         * The name of the column.
         */
        private final String name;
        /**
         * The type of the column.
         */
        private final DataType type;

        /**
         * Constructor.
         *
         * @param keyspace The Cassandra keyspace.
         * @param table    The Cassandra table name.
         * @param name     The column name.
         * @param type     The column type.
         */
        public Definition(final String keyspace, final String table, final String name, final DataType type) {
            this.keyspace = keyspace;
            this.table = table;
            this.name = name;
            this.type = type;
        }

        /**
         * Builds a column definition in an anonymous table (useful for metadata result sets built programmatically).
         *
         * @param name The column name.
         * @param type The column type.
         * @return A new column definition instance.
         */
        public static Definition buildDefinitionInAnonymousTable(final String name, final DataType type) {
            return new Definition(StringUtils.EMPTY, StringUtils.EMPTY, name, type);
        }

        /**
         * Builds a {@link ColumnSpec} instance based on this column definition.
         *
         * @param idx The index of the column in its table.
         * @return The corresponding {@link ColumnSpec} instance.
         */
        public ColumnSpec toColumnSpec(final int idx) {
            return new ColumnSpec(this.keyspace, this.table, this.name, idx,
                RawType.PRIMITIVES.get(this.type.getProtocolCode()));
        }

        @Override
        public final int hashCode() {
            return Arrays.hashCode(new Object[]{this.keyspace, this.table, this.name, this.type});
        }

        @Override
        public final boolean equals(final Object o) {
            if (!(o instanceof Definition other)) {
                return false;
            }

            return this.keyspace.equals(other.keyspace)
                && this.table.equals(other.table)
                && this.name.equals(other.name)
                && this.type.equals(other.type);
        }
    }
}
