/*
 * dw-jdbc
 * Copyright 2017 data.world, Inc.

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
package world.data.jdbc.internal.metadata;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.internal.util.Conditions.check;

/**
 * Implementation of JDBC result set metadata
 */
public final class ResultSetMetaDataImpl implements ResultSetMetaData {
    private final List<ColumnInfo> columns;

    public ResultSetMetaDataImpl(ColumnInfo... columns) {
        this(Arrays.asList(columns));
    }

    public ResultSetMetaDataImpl(List<ColumnInfo> columns) {
        this.columns = requireNonNull(columns, "columns");
        for (ColumnInfo column : columns) {
            requireNonNull(column, "column");
        }
    }

    private ColumnInfo getColumnInfo(int column) throws SQLException {
        // Remember JDBC columns use a 1 based index
        check(column >= 1 && column <= columns.size(), "Column Index is out of bounds");
        return columns.get(column - 1);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        check(isWrapperFor(iface), "Not a wrapper for the desired interface");
        return iface.cast(this);
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return getColumnInfo(column).getCatalogName();
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return getColumnInfo(column).getClassName();
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
    public String getSchemaName(int column) throws SQLException {
        return getColumnInfo(column).getSchemaName();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return getColumnInfo(column).getTableName();
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
        return getColumnInfo(column).getNullable();
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
