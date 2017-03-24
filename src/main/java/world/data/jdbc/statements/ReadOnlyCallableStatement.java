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
package world.data.jdbc.statements;

import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

interface ReadOnlyCallableStatement extends java.sql.CallableStatement, ReadOnlyPreparedStatement {
    @Override
    default Array getArray(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Array getArray(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default BigDecimal getBigDecimal(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Blob getBlob(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Blob getBlob(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default boolean getBoolean(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default boolean getBoolean(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default byte getByte(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default byte getByte(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default byte[] getBytes(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default byte[] getBytes(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Reader getCharacterStream(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Reader getCharacterStream(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Clob getClob(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Clob getClob(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Date getDate(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Date getDate(String parameterName, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Date getDate(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default double getDouble(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default double getDouble(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default float getFloat(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default float getFloat(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default int getInt(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default int getInt(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default long getLong(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default long getLong(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Reader getNCharacterStream(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Reader getNCharacterStream(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default NClob getNClob(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default NClob getNClob(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default String getNString(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default String getNString(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Object getObject(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Object getObject(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Ref getRef(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Ref getRef(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default RowId getRowId(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default RowId getRowId(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default SQLXML getSQLXML(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default short getShort(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default short getShort(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default String getString(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default String getString(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Time getTime(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Time getTime(String parameterName, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Time getTime(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Timestamp getTimestamp(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Timestamp getTimestamp(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default URL getURL(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default URL getURL(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    default boolean wasNull() throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }
}
