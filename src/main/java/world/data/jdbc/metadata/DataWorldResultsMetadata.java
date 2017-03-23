/*
* dw-jdbc
* Copyright 2016 data.world, Inc.

* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the
* License.
*
* You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
* implied. See the License for the specific language governing
* permissions and limitations under the License.
*
* This product includes software developed at data.world, Inc.(http://www.data.world/).
*/
package world.data.jdbc.metadata;


import org.apache.jena.jdbc.results.metadata.columns.ColumnInfo;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static world.data.jdbc.util.Conditions.check;

/**
 * Abstract implementation of JDBC result set metadata
 */
public class DataWorldResultsMetadata implements ResultSetMetaData {

    private final ResultSet results;
    private final List<ColumnInfo> columns = new ArrayList<>();

    /**
     * Abstract implementation of result set metadata
     *
     * @param results Result Set
     * @param columns Column information
     * @throws SQLException Thrown if metadata cannot be created
     */
    public DataWorldResultsMetadata(ResultSet results, ColumnInfo[] columns) throws SQLException {
        this.results = results;
        Collections.addAll(this.columns, columns);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        if (results != null) {
            return results.getStatement().getConnection().getCatalog();
        } else {
            return "";
        }
    }

    private ColumnInfo getColumnInfo(int column) throws SQLException {
        // Remember JDBC columns use a 1 based index
        check(column >= 1 && column <= columns.size(), "Column Index is out of bounds");
        return columns.get(column - 1);
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return getColumnInfo(column).getClassName();
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    /**
     * Gets a columns display size
     * <p>
     * Since RDF imposes no maximum on the size of a term this may be
     * arbitrarily large hence {@link Integer#MAX_VALUE} is returned, users
     * should not rely on this method to give them accurate information for UI
     * usage.
     * </p>
     */
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return getColumnInfo(column).getDisplaySize();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumnInfo(column).getLabel();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnInfo(column).getLabel();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return getColumnInfo(column).getType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumnInfo(column).getTypeName();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return getColumnInfo(column).getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return getColumnInfo(column).getScale();
    }

    @Override
    public String getSchemaName(int column) {
        // Not applicable so return empty string
        return "";
    }

    @Override
    public String getTableName(int column) {
        // Not applicable so return empty string
        return "";
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return getColumnInfo(column).isAutoIncrement();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return getColumnInfo(column).isCaseSensitive();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return getColumnInfo(column).isCurrency();
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return isWritable(column);
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return getColumnInfo(column).getNullability();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return getColumnInfo(column).isReadOnly();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return getColumnInfo(column).isSearchable();
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return getColumnInfo(column).isSigned();
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return getColumnInfo(column).isWritable();
    }
}
