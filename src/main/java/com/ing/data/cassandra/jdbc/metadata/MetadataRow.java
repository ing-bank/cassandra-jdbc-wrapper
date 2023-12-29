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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataType;
import com.ing.data.cassandra.jdbc.CassandraMetadataResultSet;
import com.ing.data.cassandra.jdbc.ColumnDefinitions;
import com.ing.data.cassandra.jdbc.ColumnDefinitions.Definition;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.UNABLE_TO_POPULATE_METADATA_ROW;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.VALID_LABELS;

/**
 * The content of a metadata row returned in a {@link CassandraMetadataResultSet}.
 */
@SuppressWarnings("unused")
public class MetadataRow {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataRow.class);

    // The 'entries' contains the ordered list of metadata values.
    private final ArrayList<Object> entries;
    // The 'names' map contains the metadata names as keys and the position of the corresponding value in the list
    // 'entries' as values. Each metadata key is a column of the metadata row.
    private final HashMap<String, Integer> names;
    private final ArrayList<Definition> definitions;

    /**
     * Constructor.
     */
    public MetadataRow() {
        this.entries = new ArrayList<>();
        this.names = new HashMap<>();
        this.definitions = new ArrayList<>();
    }

    /**
     * Add a metadata to this {@code MetadataRow} instance.
     *
     * @param key   The metadata key.
     * @param value The metadata value.
     * @param type  The metadata column type.
     * @return The updated {@code MetadataRow} instance.
     */
    public MetadataRow addEntry(final String key, final Object value, final DataType type) {
        // The 'names' map contains the metadata keys and the position of the value in the list 'entries', that's why
        // we first insert the key name with the value 'entries.size()' to define the index of the metadata value.
        this.names.put(key, this.entries.size());
        this.entries.add(value);
        this.definitions.add(new Definition(StringUtils.EMPTY, StringUtils.EMPTY, key, type));
        return this;
    }

    /**
     * Populates a metadata row defined by a row template with the specified values.
     * <p>
     *     The number of values must match the number of columns defined in the row template, otherwise a runtime
     *     exception will be thrown.
     * </p>
     *
     * @param template The row template.
     * @param values   The values used to populate the metadata row.
     * @return The updated {@code MetadataRow} instance.
     * @throws RuntimeException when the number of values does not match the number of columns defined in the row
     * template.
     */
    public MetadataRow withTemplate(final MetadataRowTemplate template, final Object... values) {
        if (template.getColumnDefinitions().length != values.length) {
            throw new RuntimeException(UNABLE_TO_POPULATE_METADATA_ROW);
        }
        for (int i = 0; i < template.getColumnDefinitions().length; i++) {
            this.addEntry(template.getColumnDefinitions()[i].getName(), values[i],
                template.getColumnDefinitions()[i].getType());
        }
        return this;
    }

    /**
     * Gets the column definitions for the metadata row.
     *
     * @return The column definitions for the metadata row.
     */
    public ColumnDefinitions getColumnDefinitions() {
        Definition[] definitionArr = new Definition[this.definitions.size()];
        definitionArr = this.definitions.toArray(definitionArr);
        return new ColumnDefinitions(definitionArr);
    }

    /**
     * Gets whether the {@code i}th column of the metadata row is {@code null}.
     *
     * @param i The column index (the first column is 0).
     * @return {@code true} if the {@code i}th column of the metadata row is {@code null}, {@code false} otherwise.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public boolean isNull(final int i) {
        return this.entries.get(i) == null;
    }

    /**
     * Gets whether the column {@code name} of the metadata row is {@code null}.
     *
     * @param name The column name.
     * @return {@code true} if the column {@code name} of the metadata row is {@code null}, {@code false} otherwise.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     */
    public boolean isNull(final String name) {
        return isNull(getIndex(name));
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@code boolean}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value. If the underlying value is {@code NULL}, the value returned is {@code false}.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public boolean getBool(final int i) {
        if (isNull(i)) {
            return false;
        }
        final Object entryValue = this.entries.get(i);
        try {
            return (Boolean) entryValue;
        } catch (final ClassCastException e) {
            LOG.warn("Unable to cast [{}] (index {}) as boolean, it will return false.", entryValue, i);
            return false;
        }
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@code boolean}.
     *
     * @param name The column name.
     * @return The metadata value. If the underlying value is {@code NULL}, the value returned is {@code false}.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     */
    public boolean getBool(final String name) {
        return getBool(getIndex(name));
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@code byte}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value. If the underlying value is {@code NULL}, the value returned is 0.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public byte getByte(final int i) {
        if (isNull(i)) {
            return 0;
        }
        final Object entryValue = this.entries.get(i);
        try {
            return (byte) entryValue;
        } catch (final ClassCastException e) {
            LOG.warn("Unable to cast [{}] (index {}) as byte, it will return 0.", entryValue, i);
            return 0;
        }
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@code byte}.
     *
     * @param name The column name.
     * @return The metadata value. If the underlying value is {@code NULL}, the value returned is 0.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     */
    public byte getByte(final String name) {
        return (byte) getInt(getIndex(name));
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@code short}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value. If the underlying value is {@code NULL}, the value returned is 0.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public short getShort(final int i) {
        if (isNull(i)) {
            return 0;
        }
        final Object entryValue = this.entries.get(i);
        try {
            return (short) entryValue;
        } catch (final ClassCastException e) {
            LOG.warn("Unable to cast [{}] (index {}) as short, it will return 0.", entryValue, i);
            return 0;
        }
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@code short}.
     *
     * @param name The column name.
     * @return The metadata value. If the underlying value is {@code NULL}, the value returned is 0.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     */
    public short getShort(final String name) {
        return (short) getInt(getIndex(name));
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@code int}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value. If the underlying value is {@code NULL}, the value returned is 0.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public int getInt(final int i) {
        if (isNull(i)) {
            return 0;
        }
        final Object entryValue = this.entries.get(i);
        try {
            return (int) entryValue;
        } catch (final ClassCastException e) {
            LOG.warn("Unable to cast [{}] (index {}) as integer, it will return 0.", entryValue, i);
            return 0;
        }
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@code int}.
     *
     * @param name The column name.
     * @return The metadata value. If the underlying value is {@code NULL}, the value returned is 0.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     */
    public Integer getInt(final String name) {
        return getInt(getIndex(name));
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@code long}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value. If the underlying value is {@code NULL}, the value returned is 0.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public long getLong(final int i) {
        if (isNull(i)) {
            return 0;
        }
        final Object entryValue = this.entries.get(i);
        try {
            return (long) entryValue;
        } catch (final ClassCastException e) {
            LOG.warn("Unable to cast [{}] (index {}) as long, it will return 0.", entryValue, i);
            return 0;
        }
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@code long}.
     *
     * @param name The column name.
     * @return The metadata value. If the underlying value is {@code NULL}, the value returned is 0.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     */
    public long getLong(final String name) {
        return getLong(getIndex(name));
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@link Date}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code date} are managed
     * by this JDBC implementation for now.
     */
    public Date getDate(final int i) {
        return null;
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@link Date}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code date} are managed
     * by this JDBC implementation for now.
     */
    public Date getDate(final String name) {
        return null;
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@link Time}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code time} are managed
     * by this JDBC implementation for now.
     */
    public Time getTime(final int i) {
        return null;
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@link Time}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code time} are managed
     * by this JDBC implementation for now.
     */
    public Time getTime(final String name) {
        return null;
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@link CqlDuration}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code duration} are
     * managed by this JDBC implementation for now.
     */
    public CqlDuration getDuration(final int i) {
        return null;
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@link CqlDuration}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code duration} are
     * managed by this JDBC implementation for now.
     */
    public CqlDuration getDuration(final String name) {
        return null;
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@code float}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @implNote Currently, this method always returns 0 since no metadata of type {@code float} are managed by this
     * JDBC implementation for now.
     */
    public float getFloat(final int i) {
        return 0;
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@link float}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @implNote Currently, this method always returns 0 since no metadata of type {@code float} are managed by this
     * JDBC implementation for now.
     */
    public float getFloat(final String name) {
        return 0;
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@code double}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @implNote Currently, this method always returns 0 since no metadata of type {@code double} are managed by this
     * JDBC implementation for now.
     */
    public double getDouble(final int i) {
        return 0;
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@link double}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @implNote Currently, this method always returns 0 since no metadata of type {@code double} are managed by this
     * JDBC implementation for now.
     */
    public double getDouble(final String name) {
        return 0;
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@link ByteBuffer}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code blob} are managed
     * by this JDBC implementation for now.
     */
    public ByteBuffer getBytes(final int i) {
        return null;
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@link ByteBuffer}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code blob} are managed
     * by this JDBC implementation for now.
     */
    public ByteBuffer getBytes(final String name) {
        return null;
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@link String}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public String getString(final int i) {
        return (String) this.entries.get(i);
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@code String}.
     *
     * @param name the column name
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     */
    public String getString(final String name) {
        return getString(getIndex(name));
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@link BigInteger}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code varint} are managed
     * by this JDBC implementation for now.
     */
    public BigInteger getVarint(final int i) {
        return null;
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@link BigInteger}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code varint} are managed
     * by this JDBC implementation for now.
     */
    public BigInteger getVarint(final String name) {
        return null;
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@link BigDecimal}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code decimal} are
     * managed by this JDBC implementation for now.
     */
    public BigDecimal getDecimal(final int i) {
        return null;
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@link BigDecimal}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code decimal} are
     * managed by this JDBC implementation for now.
     */
    public BigDecimal getDecimal(final String name) {
        return null;
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@link UUID}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code uuid} are managed
     * by this JDBC implementation for now.
     */
    public UUID getUUID(final int i) {
        return null;
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@link UUID}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code uuid} are managed
     * by this JDBC implementation for now.
     */
    public UUID getUUID(final String name) {
        return null;
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@link InetAddress}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code inet} are managed
     * by this JDBC implementation for now.
     */
    public InetAddress getInet(final int i) {
        return null;
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@link InetAddress}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code inet} are managed
     * by this JDBC implementation for now.
     */
    public InetAddress getInet(final String name) {
        return null;
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@link List}.
     *
     * @param <T>           The type of the list elements.
     * @param i             The column index (the first column is 0).
     * @param elementsClass The class of the list elements.
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code list} are managed
     * by this JDBC implementation for now.
     */
    public <T> List<T> getList(final int i, final Class<T> elementsClass) {
        return null;
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@link List}.
     *
     * @param <T>           The type of the list elements.
     * @param name          The column name.
     * @param elementsClass The class of the list elements.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code list} are managed
     * by this JDBC implementation for now.
     */
    public <T> List<T> getList(final String name, final Class<T> elementsClass) {
        return null;
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@link Set}.
     *
     * @param <T>           The type of the set elements.
     * @param i             The column index (the first column is 0).
     * @param elementsClass The class of the set elements.
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code set} are managed
     * by this JDBC implementation for now.
     */
    public <T> Set<T> getSet(final int i, final Class<T> elementsClass) {
        return null;
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@link Set}.
     *
     * @param <T>           The type of the set elements.
     * @param name          The column name.
     * @param elementsClass The class of the set elements.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code set} are managed
     * by this JDBC implementation for now.
     */
    public <T> Set<T> getSet(final String name, final Class<T> elementsClass) {
        return null;
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@link Map}.
     *
     * @param <K>           The type of the map keys.
     * @param <V>           The type of the map values.
     * @param i             The column index (the first column is 0).
     * @param keysClass     The class of the map keys.
     * @param valuesClass   The class of the map values.
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code map} are managed
     * by this JDBC implementation for now.
     */
    public <K, V> Map<K, V> getMap(final int i, final Class<K> keysClass, final Class<V> valuesClass) {
        return null;
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@link Map}.
     *
     * @param <K>           The type of the map keys.
     * @param <V>           The type of the map values.
     * @param name          The column name.
     * @param keysClass     The class of the map keys.
     * @param valuesClass   The class of the map values.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @implNote Currently, this method always returns {@code null} since no metadata of type {@code map} are managed
     * by this JDBC implementation for now.
     */
    public <K, V> Map<K, V> getMap(final String name, final Class<K> keysClass, final Class<V> valuesClass) {
        return null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (final Object entry : this.entries) {
            sb.append(entry).append(" -- ");
        }
        return "[" + sb + "]";
    }

    private Integer getIndex(final String name) {
        final Integer idx = this.names.get(name);
        if (idx == null) {
            throw new IllegalArgumentException(String.format(VALID_LABELS, name));
        }
        return idx;
    }

    /**
     * A template of metadata row.
     * <p>
     *     This is useful to define the columns of a row in a metadata result set and populate it.
     * </p>
     */
    public static class MetadataRowTemplate {

        private final Definition[] columnDefinitions;

        /**
         * Constructor.
         *
         * @param columnDefinitions The definitions of each column of the row template.
         */
        public MetadataRowTemplate(final Definition... columnDefinitions) {
            this.columnDefinitions = columnDefinitions;
        }

        /**
         * Gets the definitions of the columns in the row template.
         *
         * @return The array of columns definitions.
         */
        public Definition[] getColumnDefinitions() {
            return this.columnDefinitions;
        }

    }

}
