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
package world.data.jdbc.internal.statements;

import world.data.jdbc.DataWorldConnection;
import world.data.jdbc.DataWorldPreparedStatement;
import world.data.jdbc.internal.query.QueryEngine;
import world.data.jdbc.internal.types.NodeConversions;
import world.data.jdbc.model.Iri;
import world.data.jdbc.model.LiteralFactory;
import world.data.jdbc.model.Node;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.internal.util.Conditions.check;
import static world.data.jdbc.internal.util.Conditions.checkSupported;
import static world.data.jdbc.internal.util.Optionals.mapIfPresent;

/**
 * data.world JDBC implementation of a prepared statement
 */
public class PreparedStatementImpl extends StatementImpl implements DataWorldPreparedStatement, ReadOnlyPreparedStatement {
    private final String query;
    private final ParameterMetaData paramMetadata;
    private final Map<String, Node> params = new LinkedHashMap<>();

    /**
     * Creates a new prepared statement
     *
     * @param connection Connection
     * @throws SQLException Thrown if there is a problem preparing the statement
     */
    public PreparedStatementImpl(String query, QueryEngine queryEngine, DataWorldConnection connection,
                                 int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        super(queryEngine, connection, resultSetType, resultSetConcurrency, resultSetHoldability);
        this.query = requireNonNull(query, "query");
        this.paramMetadata = queryEngine.getParameterMetaData(query);
    }

    @Override
    public final void addBatch() throws SQLException {
        checkClosed();
        doAddBatch(query, new LinkedHashMap<>(params));
    }

    @Override
    public final void clearParameters() throws SQLException {
        checkClosed();
        params.clear();
    }

    @Override
    public final boolean execute() throws SQLException {
        checkClosed();
        return doExecuteQuery(query, params);
    }

    @Override
    public final ResultSet executeQuery() throws SQLException {
        checkClosed();
        boolean hasResultSet = doExecuteQuery(query, params);
        check(hasResultSet, "Query did not produce a result set");
        return getResultSet();
    }

    @Override
    public final ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final ParameterMetaData getParameterMetaData() throws SQLException {
        checkClosed();
        return paramMetadata;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return DataWorldPreparedStatement.class.equals(iface) || super.isWrapperFor(iface);
    }

    @Override
    public final void setArray(int parameterIndex, Array value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setAsciiStream(int parameterIndex, InputStream value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setAsciiStream(int parameterIndex, InputStream value, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setAsciiStream(int parameterIndex, InputStream value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setBigDecimal(int parameterIndex, BigDecimal value) throws SQLException {
        setParameter(parameterIndex, mapIfPresent(value, LiteralFactory::createDecimal));
    }

    @Override
    public final void setBinaryStream(int parameterIndex, InputStream value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setBinaryStream(int parameterIndex, InputStream value, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setBinaryStream(int parameterIndex, InputStream value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setBlob(int parameterIndex, Blob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setBlob(int parameterIndex, InputStream value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setBlob(int parameterIndex, InputStream value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setBoolean(int parameterIndex, boolean value) throws SQLException {
        setParameter(parameterIndex, LiteralFactory.createBoolean(value));
    }

    @Override
    public final void setByte(int parameterIndex, byte value) throws SQLException {
        // Map to closest common SPARQL literal datatype, one of xsd:integer, xsd:decimal
        setParameter(parameterIndex, LiteralFactory.createInteger(value));
    }

    @Override
    public final void setBytes(int parameterIndex, byte[] value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setCharacterStream(int parameterIndex, Reader value, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setClob(int parameterIndex, Clob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setClob(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setClob(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    @SuppressWarnings("deprecation")
    public final void setDate(int parameterIndex, Date value) throws SQLException {
        setParameter(parameterIndex, mapIfPresent(value, LiteralFactory::createDate));
    }

    @Override
    @SuppressWarnings("deprecation")
    public final void setDate(int parameterIndex, Date value, Calendar calendar) throws SQLException {
        setParameter(parameterIndex, mapIfPresent2(value, calendar, LiteralFactory::createDate, LiteralFactory::createDate));
    }

    @Override
    public final void setDouble(int parameterIndex, double value) throws SQLException {
        // Map to closest common SPARQL literal datatype, one of xsd:integer, xsd:decimal
        setParameter(parameterIndex, LiteralFactory.createDecimal(value));
    }

    @Override
    public final void setFloat(int parameterIndex, float value) throws SQLException {
        // Map to closest common SPARQL literal datatype, one of xsd:integer, xsd:decimal
        setParameter(parameterIndex, LiteralFactory.createDecimal(value));
    }

    @Override
    public final void setInt(int parameterIndex, int value) throws SQLException {
        // Map to closest common SPARQL literal datatype, one of xsd:integer, xsd:decimal
        setParameter(parameterIndex, LiteralFactory.createInteger(value));
    }

    @Override
    public final void setLong(int parameterIndex, long value) throws SQLException {
        // Map to closest common SPARQL literal datatype, one of xsd:integer, xsd:decimal
        setParameter(parameterIndex, LiteralFactory.createInteger(value));
    }

    @Override
    public final void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setNClob(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setNClob(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setNString(int parameterIndex, String value) throws SQLException {
        setParameter(parameterIndex, mapIfPresent(value, LiteralFactory::createString));
    }

    @Override
    public final void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParameter(parameterIndex, null);
    }

    @Override
    public final void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setParameter(parameterIndex, null);
    }

    @Override
    public final void setObject(int parameterIndex, Object value) throws SQLException {
        setParameter(parameterIndex, NodeConversions.toNode(value));
    }

    @Override
    public final void setObject(int parameterIndex, Object value, int targetSqlType) throws SQLException {
        setParameter(parameterIndex, NodeConversions.toNode(value, targetSqlType));
    }

    @Override
    public final void setObject(int parameterIndex, Object value, int targetSqlType, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setObject(int parameterIndex, Object value, SQLType targetSqlType) throws SQLException {
        checkSupported("java.sql".equals(targetSqlType.getVendor()));  // see java.sql.JDBCType
        setObject(parameterIndex, value, targetSqlType.getVendorTypeNumber());
    }

    @Override
    public final void setObject(int parameterIndex, Object value, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        checkSupported("java.sql".equals(targetSqlType.getVendor()));  // see java.sql.JDBCType
        setObject(parameterIndex, value, targetSqlType.getVendorTypeNumber(), scaleOrLength);
    }

    @Override
    public final void setRef(int parameterIndex, Ref value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setRowId(int parameterIndex, RowId value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setSQLXML(int parameterIndex, SQLXML value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final void setShort(int parameterIndex, short value) throws SQLException {
        // Map to closest common SPARQL literal datatype, one of xsd:integer, xsd:decimal
        setParameter(parameterIndex, LiteralFactory.createInteger(value));
    }

    @Override
    public final void setString(int parameterIndex, String value) throws SQLException {
        setParameter(parameterIndex, mapIfPresent(value, LiteralFactory::createString));
    }

    @Override
    @SuppressWarnings("deprecation")
    public final void setTime(int parameterIndex, Time value) throws SQLException {
        setParameter(parameterIndex, mapIfPresent(value, LiteralFactory::createTime));
    }

    @Override
    @SuppressWarnings("deprecation")
    public final void setTime(int parameterIndex, Time value, Calendar calendar) throws SQLException {
        setParameter(parameterIndex, mapIfPresent2(value, calendar, LiteralFactory::createTime, LiteralFactory::createTime));
    }

    @Override
    @SuppressWarnings("deprecation")
    public final void setTimestamp(int parameterIndex, Timestamp value) throws SQLException {
        setParameter(parameterIndex, mapIfPresent(value, LiteralFactory::createDateTime));
    }

    @Override
    @SuppressWarnings("deprecation")
    public final void setTimestamp(int parameterIndex, Timestamp value, Calendar calendar) throws SQLException {
        setParameter(parameterIndex, mapIfPresent2(value, calendar, LiteralFactory::createDateTime, LiteralFactory::createDateTime));
    }

    @Override
    public final void setURL(int parameterIndex, URL value) throws SQLException {
        setParameter(parameterIndex, value != null ? new Iri(value.toString()) : null);
    }

    @Deprecated
    @Override
    public final void setUnicodeStream(int parameterIndex, InputStream value, int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private void setParameter(int parameterIndex, Node n) throws SQLException {
        checkClosed();
        queryEngine.checkPositionalParametersSupported();
        check(parameterIndex >= 1 && parameterIndex <= paramMetadata.getParameterCount(), "Parameter Index is out of bounds");
        params.put("$data_world_param" + (parameterIndex - 1), n);
    }

    void setParameter(String parameterName, Node n) throws SQLException {
        checkClosed();
        queryEngine.checkNamedParametersSupported();
        check(parameterName != null && !parameterName.isEmpty(), "Empty or null parameter name");
        check(!parameterName.startsWith("data_world_param"), "May not set positional parameter values using named parameter methods");
        params.put("$" + parameterName, n);
    }

    static <T, U> Node mapIfPresent2(T t, U u, Function<T, Node> fn1, BiFunction<T, U, Node> fn2) {
        if (t == null) {
            return null;
        } else if (u == null) {
            return fn1.apply(t);
        } else {
            return fn2.apply(t, u);
        }
    }
}
