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

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Metadata describing the columns returned in a {@link CassandraResultSet} or a
 * {@link CassandraPreparedStatement}.
 * <p>
 * A {@code columnDefinitions}} instance is mainly a list of
 * {@code ColumnsDefinitions.Definition}. The definitions or metadata for a column
 * can be accessed either by:
 * <ul>
 * <li>index (indexed from 0)</li>
 * <li>name</li>
 * </ul>
 * <p>
 * When accessed by name, column selection is case insensitive. In case multiple
 * columns only differ by the case of their name, then the column returned with
 * be the first column that has been defined in CQL without forcing case sensitivity
 * (that is, it has either been defined without quotes or is fully lowercase).
 * If none of the columns have been defined in this manner, the first column matching
 * (with case insensitivity) is returned. You can force the case of a selection
 * by double quoting the name.
 * <p>
 * For example:
 * <ul>
 * <li>If {@code cd} contains column {@code fOO}, then {@code cd.contains("foo")},
 * {@code cd.contains("fOO")} and {@code cd.contains("Foo")} will return {@code true}.</li>
 * <li>If {@code cd} contains both {@code foo} and {@code FOO} then:
 * <ul>
 * <li>{@code cd.getType("foo")}, {@code cd.getType("fOO")} and {@code cd.getType("FOO")}
 * will all match column {@code foo}.</li>
 * <li>{@code cd.getType("\"FOO\"")} will match column {@code FOO}</li>
 * </ul>
 * </ul>
 * Note that the preceding rules mean that if a {@code ColumnDefinitions} object
 * contains multiple occurrences of the exact same name (be it the same column
 * multiple times or columns from different tables with the same name), you
 * will have to use selection by index to disambiguate.
 */
public class ColumnDefinitions implements Iterable<ColumnDefinitions.Definition> {

    private final Definition[] byIdx;
    private final Map<String, int[]> byName;

    /**
     * Constructor.
     *
     * @param defs Array of columns definitions.
     */
    public ColumnDefinitions(final Definition[] defs) {
        this.byIdx = defs;
        this.byName = new HashMap<>(defs.length);

        for (int i = 0; i < defs.length; i++) {
            // Be optimistic, 99% of the time, previous will be null.
            final int[] previous = this.byName.put(defs[i].name.toLowerCase(), new int[]{i});
            if (previous != null) {
                final int[] indexes = new int[previous.length + 1];
                System.arraycopy(previous, 0, indexes, 0, previous.length);
                indexes[indexes.length - 1] = i;
                this.byName.put(defs[i].name.toLowerCase(), indexes);
            }
        }
    }

    /**
     * Returns the number of columns described by this {@code Columns}
     * instance.
     *
     * @return the number of columns described by this metadata.
     */
    public int size() {
        return byIdx.length;
    }

    /**
     * Returns whether this metadata contains a given name.
     *
     * @param name the name to check.
     * @return {@code true} if this metadata contains the column named {@code name},
     * {@code false} otherwise.
     */
    public boolean contains(final String name) {
        return findAllIdx(name) != null;
    }

    /**
     * The first index in this metadata of the provided name, if present.
     *
     * @param name the name of the column.
     * @return the index of the first occurrence of {@code name} in this metadata if
     * {@code contains(name)}, -1 otherwise.
     */
    public int getIndexOf(final String name) {
        return findFirstIdx(name);
    }

    /**
     * Returns an iterator over the {@link Definition} contained in this metadata.
     * <p>
     * The order of the iterator will be the one of this metadata.
     *
     * @return an iterator over the {@link Definition} contained in this metadata.
     */
    @Override
    @Nonnull
    public Iterator<Definition> iterator() {
        return Arrays.asList(byIdx).iterator();
    }

    /**
     * Returns a list containing all the definitions of this metadata in order.
     *
     * @return a list of the {@link Definition} contained in this metadata.
     */
    public List<Definition> asList() {
        return Arrays.asList(byIdx);
    }

    /**
     * Returns the name of the {@code i}th column in this metadata.
     *
     * @param i the index in this metadata.
     * @return the name of the {@code i}th column in this metadata.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}
     */
    public String getName(final int i) {
        return byIdx[i].name;
    }

    /**
     * Returns the type of the {@code i}th column in this metadata.
     *
     * @param i the index in this metadata.
     * @return the type of the {@code i}th column in this metadata.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}
     */
    public DataType getType(final int i) {
        return byIdx[i].type;
    }

    /**
     * Returns the type of the first occurrence of {@code name} in this metadata.
     *
     * @param name the name of the column.
     * @return the type of (the first occurrence of) {@code name} in this metadata.
     * @throws IllegalArgumentException if {@code name} is not in this metadata.
     */
    public DataType getType(final String name) {
        return getType(getFirstIdx(name));
    }

    /**
     * Returns the keyspace of the {@code i}th column in this metadata.
     *
     * @param i the index in this metadata.
     * @return the keyspace of the {@code i}th column in this metadata.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}
     */
    public String getKeyspace(final int i) {
        return byIdx[i].keyspace;
    }

    /**
     * Returns the keyspace of the first occurrence of {@code name} in this metadata.
     *
     * @param name the name of the column.
     * @return the keyspace of (the first occurrence of) column {@code name} in this metadata.
     * @throws IllegalArgumentException if {@code name} is not in this metadata.
     */
    public String getKeyspace(final String name) {
        return getKeyspace(getFirstIdx(name));
    }

    /**
     * Returns the table of the {@code i}th column in this metadata.
     *
     * @param i the index in this metadata.
     * @return the table of the {@code i}th column in this metadata.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}
     */
    public String getTable(final int i) {
        return byIdx[i].table;
    }

    /**
     * Returns the table of first occurrence of {@code name} in this metadata.
     *
     * @param name the name of the column.
     * @return the table of (the first occurrence of) column {@code name} in this metadata.
     * @throws IllegalArgumentException if {@code name} is not in this metadata.
     */
    public String getTable(final String name) {
        return getTable(getFirstIdx(name));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Columns[");
        for (int i = 0; i < size(); i++) {
            if (i != 0)
                sb.append(", ");
            final Definition def = byIdx[i];
            sb.append(def.name).append('(').append(def.type).append(')');
        }
        sb.append(']');
        return sb.toString();
    }

    int findFirstIdx(final String name) {
        final int[] indexes = findAllIdx(name);
        return indexes == null ? -1 : indexes[0];
    }

    int[] findAllIdx(String name) {
        boolean caseSensitive = false;
        if (name.length() >= 2 && name.charAt(0) == '"' && name.charAt(name.length() - 1) == '"') {
            name = name.substring(1, name.length() - 1);
            caseSensitive = true;
        }

        final int[] indexes = byName.get(name.toLowerCase());
        if (!caseSensitive || indexes == null) {
            return indexes;
        }

        // First, optimistic and assume all are matching
        int nbMatch = 0;
        for (final int index : indexes) {
            if (name.equals(byIdx[index].name)) {
                nbMatch++;
            }
        }

        if (nbMatch == indexes.length) {
            return indexes;
        }

        final int[] result = new int[nbMatch];
        int j = 0;
        for (final int idx : indexes) {
            if (name.equals(byIdx[idx].name)) {
                result[j++] = idx;
            }
        }

        return result;
    }

    int[] getAllIdx(final String name) {
        final int[] indexes = findAllIdx(name);
        if (indexes == null) {
            throw new IllegalArgumentException(name + " is not a column defined in this metadata");
        }
        return indexes;
    }

    int getFirstIdx(final String name) {
        return getAllIdx(name)[0];
    }

    /**
     * A column definition.
     */
    public static class Definition {

        private final String keyspace;
        private final String table;
        private final String name;
        private final DataType type;

        /**
         * Constructor.
         *
         * @param keyspace The Cassandra keyspace.
         * @param table    The Cassandra table.
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
         * The name of the keyspace this column is part of.
         *
         * @return the name of the keyspace this column is part of.
         */
        public String getKeyspace() {
            return keyspace;
        }

        /**
         * Returns the name of the table this column is part of.
         *
         * @return the name of the table this column is part of.
         */
        public String getTable() {
            return table;
        }

        /**
         * Returns the name of the column.
         *
         * @return the name of the column.
         */
        public String getName() {
            return name;
        }

        /**
         * Returns the type of the column.
         *
         * @return the type of the column.
         */
        public DataType getType() {
            return type;
        }

        @Override
        public final int hashCode() {
            return Arrays.hashCode(new Object[]{keyspace, table, name, type});
        }

        @Override
        public final boolean equals(final Object o) {
            if (!(o instanceof Definition)) {
                return false;
            }

            final Definition other = (Definition) o;
            return keyspace.equals(other.keyspace)
                && table.equals(other.table)
                && name.equals(other.name)
                && type.equals(other.type);
        }
    }
}
