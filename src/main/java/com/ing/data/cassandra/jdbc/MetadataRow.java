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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ing.data.cassandra.jdbc.ColumnDefinitions.Definition;
import org.apache.commons.lang3.StringUtils;

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

/**
 * The content of a metadata row returned in a {@link CassandraMetadataResultSet}.
 */
public class MetadataRow {

    // The 'entries' contains the ordered list of metadata values.
    private final ArrayList<String> entries;
    // The 'names' map contains the metadata names as keys and the position of the corresponding value in the list
    // 'entries' as values. Each metadata key is a column of the metadata row.
    private final HashMap<String, Integer> names;
    private final ArrayList<ColumnDefinitions.Definition> definitions;

    /**
     * Constructor.
     */
    public MetadataRow() {
        this.entries = Lists.newArrayList();
        this.names = Maps.newHashMap();
        this.definitions = Lists.newArrayList();
    }

    /**
     * Add a metadata to this {@code MetadataRow} instance.
     *
     * @param key   The metadata key.
     * @param value The metadata value.
     * @return The updated {@code MetadataRow} instance.
     */
    public MetadataRow addEntry(final String key, final String value) {
        // The 'names' map contains the metadata keys and the position of the value in the list 'entries', that's why
        // we first insert the key name with the value 'entries.size()' to define the index of the metadata value.
        this.names.put(key, this.entries.size());
        this.entries.add(value);
        this.definitions.add(new Definition(StringUtils.EMPTY, StringUtils.EMPTY, key, DataTypes.TEXT));
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
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public boolean getBool(final int i) {
        return Boolean.parseBoolean(this.entries.get(i));
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@code boolean}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     */
    public boolean getBool(final String name) {
        return getBool(getIndex(name));
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@code byte}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public byte getByte(final int i) {
        return (byte) getInt(i);
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@code byte}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     */
    public byte getByte(final String name) {
        return (byte) getInt(getIndex(name));
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@code short}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     */
    public short getShort(final int i) {
        return (short) getInt(i);
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@code short}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     */
    public short getShort(final String name) {
        return (short) getInt(getIndex(name));
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@code int}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @throws NumberFormatException if the value is not a parsable {@code int} value.
     */
    public int getInt(final int i) {
        return Integer.parseInt(this.entries.get(i));
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@code int}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @throws NumberFormatException if the value is not a parsable {@code int} value.
     */
    public int getInt(final String name) {
        return getInt(getIndex(name));
    }

    /**
     * Retrieves the value of the {@code i}th column of the metadata row as {@code long}.
     *
     * @param i The column index (the first column is 0).
     * @return The metadata value.
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}.
     * @throws NumberFormatException if the value is not a parsable {@code long} value.
     */
    public long getLong(final int i) {
        return Long.parseLong(this.entries.get(i));
    }

    /**
     * Retrieves the value of the column {@code name} of the metadata row as {@code long}.
     *
     * @param name The column name.
     * @return The metadata value.
     * @throws IllegalArgumentException if {@code name} is not a valid metadata name for this row.
     * @throws NumberFormatException if the value is not a parsable {@code long} value.
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
        return this.entries.get(i);
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
     * @param i The column index (the first column is 0).
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
     * @param name The column name.
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
     * @param i The column index (the first column is 0).
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
     * @param name The column name.
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
     * @param i The column index (the first column is 0).
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
     * @param name The column name.
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
        for (final String entry : this.entries) {
            sb.append(entry).append(" -- ");
        }
        return "[" + sb.toString() + "]";
    }

    private Integer getIndex(final String name) {
        final Integer idx = this.names.get(name);
        if (idx == null) {
            throw new IllegalArgumentException(name + " is not a column defined in this row.");
        }
        return idx;
    }

}
