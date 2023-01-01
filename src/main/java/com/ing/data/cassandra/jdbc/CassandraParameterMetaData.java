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

import com.datastax.oss.driver.api.core.cql.BoundStatement;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Cassandra parameter metadata: implementation class for {@link ParameterMetaData}.
 */
public class CassandraParameterMetaData implements ParameterMetaData {

    private final BoundStatement boundStatement;
    private final int parametersCount;

    /**
     * Constructor.
     *
     * @param boundStatement   The CQL bound statement from a {@link CassandraPreparedStatement}.
     * @param parametersCount  The number of parameters in the prepared statement.
     */
    public CassandraParameterMetaData(final BoundStatement boundStatement, final int parametersCount) {
        this.boundStatement = boundStatement;
        this.parametersCount = parametersCount;
    }

    private String getParameterCqlType(final int i) {
        return DataTypeEnum.cqlName(this.boundStatement.getType(i));
    }

    private AbstractJdbcType<?> getParameterJdbcType(final int i) {
        return TypesMap.getTypeForComparator(getParameterCqlType(i).toLowerCase());
    }

    @Override
    public int getParameterCount() throws SQLException {
        return this.parametersCount;
    }

    /**
     * Retrieves whether null values are allowed in the designated parameter.
     * <p>
     *     All columns are nullable in Cassandra so any parameter allows null values.
     * </p>
     *
     * @param i The parameter index considering the first parameter is 1, the second is 2, ...
     * @return The nullability status of the given parameter; always {@link ParameterMetaData#parameterNullable} in
     * this implementation.
     */
    @Override
    public int isNullable(int i) {
        // Note: absence is the equivalent of null in Cassandra
        return ParameterMetaData.parameterNullable;
    }

    @Override
    public boolean isSigned(int i) throws SQLException {
        return getParameterJdbcType(i).isSigned();
    }

    @Override
    public int getPrecision(int i) throws SQLException {
        return getParameterJdbcType(i).getPrecision(null);
    }

    @Override
    public int getScale(int i) throws SQLException {
        return getParameterJdbcType(i).getScale(null);
    }

    @Override
    public int getParameterType(int i) throws SQLException {
        return getParameterJdbcType(i).getJdbcType();
    }

    @Override
    public String getParameterTypeName(int i) throws SQLException {
        return getParameterCqlType(i);
    }

    @Override
    public String getParameterClassName(int i) throws SQLException {
        return getParameterJdbcType(i).getType().getName();
    }

    /**
     * Retrieves the designated parameter's mode.
     * <p>
     *     Since Cassandra only supports prepared statements and not callable statements, the parameter's mode is
     *     always IN.
     * </p>
     *
     * @param i The parameter index considering the first parameter is 1, the second is 2, ...
     * @return The mode of the parameter; always {@link ParameterMetaData#parameterModeIn} in this implementation.
     */
    @Override
    public int getParameterMode(int i) {
        return ParameterMetaData.parameterModeIn;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        if (aClass.isAssignableFrom(getClass())) {
            return aClass.cast(this);
        }
        throw new SQLFeatureNotSupportedException(String.format(Utils.NO_INTERFACE, aClass.getSimpleName()));
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return aClass.isAssignableFrom(getClass());
    }
}
