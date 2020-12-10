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

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ing.data.cassandra.jdbc.ColumnDefinitions.Definition;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class MetadataRow {

    private final ArrayList<String> entries;
    private final HashMap<String, Integer> names;
    private final ArrayList<ColumnDefinitions.Definition> definitions;

    public MetadataRow() {
        entries = Lists.newArrayList();
        names = Maps.newHashMap();
        definitions = Lists.newArrayList();
    }

    public MetadataRow addEntry(final String key, final String value) {
        names.put(key, entries.size());
        entries.add(value);
        definitions.add(new Definition(StringUtils.EMPTY, StringUtils.EMPTY, key, DataTypes.TEXT));
        return this;
    }

    public UdtValue getUDTValue(final int i) {
        return null;
    }

    public TupleValue getTupleValue(final int i) {
        return null;
    }

    public UdtValue getUDTValue(final String name) {
        return null;
    }

    public TupleValue getTupleValue(final String name) {
        return null;
    }

    public ColumnDefinitions getColumnDefinitions() {
        Definition[] definitionArr = new Definition[definitions.size()];
        definitionArr = definitions.toArray(definitionArr);
        return new ColumnDefinitions(definitionArr);
    }

    public boolean isNull(final int i) {
        return entries.get(i) == null;
    }

    public boolean isNull(final String name) {
        return isNull(names.get(name));
    }

    public boolean getBool(final int i) {
        return Boolean.parseBoolean(entries.get(i));
    }

    public boolean getBool(final String name) {
        return getBool(names.get(name));
    }

    public int getInt(final int i) {
        return Integer.parseInt(entries.get(i));
    }

    public int getInt(final String name) {
        return getInt(names.get(name));
    }

    public long getLong(final int i) {
        return Long.parseLong(entries.get(i));
    }

    public long getLong(final String name) {
        return getLong(names.get(name));
    }

    public Date getDate(final int i) {
        return null;
    }

    public Date getDate(final String name) {
        return null;
    }

    public float getFloat(final int i) {
        return 0;
    }

    public float getFloat(final String name) {
        return 0;
    }

    public double getDouble(final int i) {
        return 0;
    }

    public double getDouble(final String name) {
        return 0;
    }

    public ByteBuffer getBytesUnsafe(final int i) {
        return null;
    }

    public ByteBuffer getBytesUnsafe(final String name) {
        return null;
    }

    public ByteBuffer getBytes(final int i) {
        return null;
    }

    public ByteBuffer getBytes(final String name) {
        return null;
    }

    public String getString(final int i) {
        return entries.get(i);
    }

    public String getString(final String name) {
        return getString(names.get(name));
    }

    public BigInteger getVarint(final int i) {
        return null;
    }

    public BigInteger getVarint(final String name) {
        return null;
    }

    public BigDecimal getDecimal(final int i) {
        return null;
    }

    public BigDecimal getDecimal(final String name) {
        return null;
    }

    public UUID getUUID(final int i) {
        return null;
    }

    public UUID getUUID(final String name) {
        return null;
    }

    public InetAddress getInet(final int i) {
        return null;
    }

    public InetAddress getInet(final String name) {
        return null;
    }

    public <T> List<T> getList(final int i, final Class<T> elementsClass) {
        return null;
    }

    public <T> List<T> getList(final String name, final Class<T> elementsClass) {
        return null;
    }

    public <T> Set<T> getSet(final int i, final Class<T> elementsClass) {
        return null;
    }

    public <T> Set<T> getSet(final String name, final Class<T> elementsClass) {
        return null;
    }

    public <K, V> Map<K, V> getMap(final int i, final Class<K> keysClass, final Class<V> valuesClass) {
        return null;
    }

    public <K, V> Map<K, V> getMap(final String name, final Class<K> keysClass, final Class<V> valuesClass) {
        return null;
    }

    public String toString() {
        final StringBuilder builder = new StringBuilder();
        for (final String entry : entries) {
            builder.append(entry + " -- ");
        }
        return "[" + builder.toString() + "]";
    }
}
