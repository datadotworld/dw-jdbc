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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.iri.IRI;
import org.apache.jena.jdbc.utils.JdbcNodeUtils;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import world.data.jdbc.connections.DataWorldConnection;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URI;
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
import java.sql.Types;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.util.Conditions.check;
import static world.data.jdbc.util.Conditions.checkSupported;

/**
 * data.world JDBC implementation of a prepared statement
 */
public class DataWorldPreparedStatement extends DataWorldStatement implements ReadOnlyPreparedStatement {
    private final String query;
    private final ParameterMetaData paramMetadata;
    private final Map<String, Node> params = new LinkedHashMap<>();

    /**
     * Creates a new prepared statement
     *
     * @param connection Connection
     * @throws SQLException Thrown if there is a problem preparing the statement
     */
    public DataWorldPreparedStatement(String query, DataWorldConnection connection, QueryBuilder queryBuilder) throws SQLException {
        super(connection, queryBuilder);
        this.query = requireNonNull(query, "query");
        this.paramMetadata = queryBuilder.buildParameterMetadata(query);
    }

    @Override
    public void addBatch() throws SQLException {
        addBatch(query);
    }

    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        params.clear();
    }

    @Override
    public boolean execute() throws SQLException {
        return execute(query);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(query);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        // Return null because we don't know in advance the column types
        return null;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkClosed();
        return paramMetadata;
    }

    @Override
    public void setArray(int parameterIndex, Array value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream value, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal value) throws SQLException {
        setParameter(parameterIndex, value != null ? LiteralFactory.bigDecimalToNode(value) : null);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream value, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBoolean(int parameterIndex, boolean value) throws SQLException {
        setParameter(parameterIndex, LiteralFactory.booleanToNode(value));
    }

    @Override
    public void setByte(int parameterIndex, byte value) throws SQLException {
        setParameter(parameterIndex, LiteralFactory.byteToNode(value));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader value, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Clob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(int parameterIndex, Date value) throws SQLException {
        setParameter(parameterIndex, value != null ? LiteralFactory.dateTimeToNode(value) : null);
    }

    @Override
    public void setDate(int parameterIndex, Date value, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDouble(int parameterIndex, double value) throws SQLException {
        setParameter(parameterIndex, LiteralFactory.doubleToNode(value));
    }

    @Override
    public void setFloat(int parameterIndex, float value) throws SQLException {
        setParameter(parameterIndex, LiteralFactory.floatToNode(value));
    }

    @Override
    public void setInt(int parameterIndex, int value) throws SQLException {
        setParameter(parameterIndex, NodeFactoryExtra.intToNode(value));
    }

    @Override
    public void setLong(int parameterIndex, long value) throws SQLException {
        setParameter(parameterIndex, NodeFactoryExtra.intToNode(value));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setParameter(parameterIndex, value != null ? NodeFactory.createLiteral(value) : null);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParameter(parameterIndex, null);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setParameter(parameterIndex, null);
    }

    @Override
    public void setObject(int parameterIndex, Object value) throws SQLException {
        setParameter(parameterIndex, objectToNode(value));
    }

    Node objectToNode(Object value) throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof Node) {
            return (Node) value;
        } else if (value instanceof RDFNode) {
            return ((RDFNode) value).asNode();
        } else if (value instanceof String) {
            return NodeFactory.createLiteral((String) value);
        } else if (value instanceof Boolean) {
            return LiteralFactory.booleanToNode((Boolean) value);
        } else if (value instanceof Long) {
            return NodeFactoryExtra.intToNode((Long) value);
        } else if (value instanceof Integer) {
            return NodeFactoryExtra.intToNode((Integer) value);
        } else if (value instanceof Short) {
            return LiteralFactory.shortToNode((Short) value);
        } else if (value instanceof Byte) {
            return LiteralFactory.byteToNode((Byte) value);
        } else if (value instanceof BigDecimal) {
            return LiteralFactory.bigDecimalToNode((BigDecimal) value);
        } else if (value instanceof Float) {
            return LiteralFactory.floatToNode((Float) value);
        } else if (value instanceof Double) {
            return LiteralFactory.doubleToNode((Double) value);
        } else if (value instanceof Date) {
            return LiteralFactory.dateTimeToNode((Date) value);
        } else if (value instanceof Time) {
            return LiteralFactory.timeToNode((Time) value);
        } else if (value instanceof Calendar) {
            return NodeFactoryExtra.dateTimeToNode((Calendar) value);
        } else if (value instanceof URL) {
            return NodeFactory.createURI(value.toString());
        } else if (value instanceof URI) {
            return NodeFactory.createURI(value.toString());
        } else if (value instanceof IRI) {
            return NodeFactory.createURI(value.toString());
        } else {
            throw new SQLException("setObject() received a value that could not be converted to a RDF node for use in a SPARQL query");
        }
    }

    @Override
    public void setObject(int parameterIndex, Object value, int targetSqlType) throws SQLException {
        setParameter(parameterIndex, objectToNode(value, targetSqlType));
    }

    Node objectToNode(Object value, int targetSqlType) throws SQLException {
        if (value == null) {
            return null;
        }
        try {
            switch (targetSqlType) {
                case Types.ARRAY:
                case Types.BINARY:
                case Types.BIT:
                case Types.BLOB:
                case Types.CLOB:
                case Types.DATALINK:
                case Types.DISTINCT:
                case Types.LONGNVARCHAR:
                case Types.LONGVARBINARY:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NCLOB:
                case Types.NULL:
                case Types.NUMERIC:
                case Types.OTHER:
                case Types.REAL:
                case Types.REF:
                case Types.ROWID:
                case Types.SQLXML:
                case Types.STRUCT:
                case Types.VARBINARY:
                    throw new SQLException("The provided SQL Target Type cannot be translated into an appropriate RDF term type");
                case Types.BIGINT:
                    if (value instanceof Long) {
                        return NodeFactoryExtra.intToNode((Long) value);
                    } else if (value instanceof Integer) {
                        return NodeFactoryExtra.intToNode((long) (Integer) value);
                    } else if (value instanceof Short) {
                        return NodeFactoryExtra.intToNode((long) (Short) value);
                    } else if (value instanceof Byte) {
                        return NodeFactoryExtra.intToNode((long) (Byte) value);
                    } else if (value instanceof Node) {
                        return NodeFactoryExtra.intToNode(JdbcNodeUtils.toLong((Node) value));
                    } else if (value instanceof String) {
                        return NodeFactoryExtra.intToNode(Long.parseLong((String) value));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                case Types.BOOLEAN:
                    if (value instanceof Boolean) {
                        return LiteralFactory.booleanToNode((Boolean) value);
                    } else if (value instanceof Node) {
                        return LiteralFactory.booleanToNode(JdbcNodeUtils.toBoolean((Node) value));
                    } else if (value instanceof String) {
                        return LiteralFactory.booleanToNode(Boolean.parseBoolean((String) value));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                case Types.DATE:
                    if (value instanceof Date) {
                        return LiteralFactory.dateTimeToNode((Date) value);
                    } else if (value instanceof Node) {
                        return LiteralFactory.dateTimeToNode(JdbcNodeUtils.toDate((Node) value));
                    } else if (value instanceof Time) {
                        return LiteralFactory.timeToNode((Time) value);
                    } else if (value instanceof Calendar) {
                        return NodeFactoryExtra.dateTimeToNode((Calendar) value);
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                case Types.DECIMAL:
                    if (value instanceof BigDecimal) {
                        return LiteralFactory.bigDecimalToNode((BigDecimal) value);
                    } else if (value instanceof Node) {
                        return LiteralFactory.bigDecimalToNode(JdbcNodeUtils.toDecimal((Node) value));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                case Types.DOUBLE:
                    if (value instanceof Double) {
                        return LiteralFactory.doubleToNode((Double) value);
                    } else if (value instanceof Float) {
                        return LiteralFactory.doubleToNode((Float) value);
                    } else if (value instanceof Node) {
                        return LiteralFactory.doubleToNode(JdbcNodeUtils.toDouble((Node) value));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                case Types.FLOAT:
                    if (value instanceof Float) {
                        return LiteralFactory.floatToNode((Float) value);
                    } else if (value instanceof Node) {
                        return LiteralFactory.floatToNode(JdbcNodeUtils.toFloat((Node) value));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                case Types.INTEGER:
                    if (value instanceof Integer) {
                        return NodeFactoryExtra.intToNode((Integer) value);
                    } else if (value instanceof Short) {
                        return NodeFactoryExtra.intToNode((Short) value);
                    } else if (value instanceof Byte) {
                        return NodeFactoryExtra.intToNode((Byte) value);
                    } else if (value instanceof Node) {
                        return NodeFactoryExtra.intToNode(JdbcNodeUtils.toInt((Node) value));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                case Types.JAVA_OBJECT:
                    if (value instanceof Node) {
                        return (Node) value;
                    } else if (value instanceof RDFNode) {
                        return ((RDFNode) value).asNode();
                    } else if (value instanceof String) {
                        return NodeFactory.createLiteral((String) value);
                    } else if (value instanceof URL) {
                        return NodeFactory.createURI(value.toString());
                    } else if (value instanceof URI) {
                        return NodeFactory.createURI(value.toString());
                    } else if (value instanceof IRI) {
                        return NodeFactory.createURI(value.toString());
                    } else if (value instanceof BigDecimal) {
                        return LiteralFactory.bigDecimalToNode((BigDecimal) value);
                    } else if (value instanceof Boolean) {
                        return LiteralFactory.booleanToNode((Boolean) value);
                    } else if (value instanceof Byte) {
                        return LiteralFactory.byteToNode((Byte) value);
                    } else if (value instanceof Double) {
                        return LiteralFactory.doubleToNode((Double) value);
                    } else if (value instanceof Float) {
                        return LiteralFactory.floatToNode((Float) value);
                    } else if (value instanceof Short) {
                        return LiteralFactory.shortToNode((Short) value);
                    } else if (value instanceof Integer) {
                        return NodeFactoryExtra.intToNode((Integer) value);
                    } else if (value instanceof Long) {
                        return NodeFactoryExtra.intToNode((Long) value);
                    } else if (value instanceof Date) {
                        return LiteralFactory.dateTimeToNode((Date) value);
                    } else if (value instanceof Time) {
                        return LiteralFactory.timeToNode((Time) value);
                    } else if (value instanceof Calendar) {
                        return NodeFactoryExtra.dateTimeToNode((Calendar) value);
                    } else {
                        return NodeFactory.createLiteral(value.toString());
                    }
                case Types.CHAR:
                case Types.NVARCHAR:
                case Types.VARCHAR:
                    return NodeFactory.createLiteral(value.toString());
                case Types.SMALLINT:
                    if (value instanceof Short) {
                        return LiteralFactory.shortToNode((Short) value);
                    } else if (value instanceof Byte) {
                        return LiteralFactory.shortToNode((Byte) value);
                    } else if (value instanceof Node) {
                        return LiteralFactory.shortToNode(JdbcNodeUtils.toShort((Node) value));
                    } else if (value instanceof String) {
                        return LiteralFactory.shortToNode(Short.parseShort((String) value));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired type");
                    }
                case Types.TIME:
                    if (value instanceof Time) {
                        return LiteralFactory.timeToNode((Time) value);
                    } else if (value instanceof Node) {
                        return LiteralFactory.timeToNode(JdbcNodeUtils.toDate((Node) value));
                    } else if (value instanceof Date) {
                        return LiteralFactory.timeToNode((Date) value);
                    } else if (value instanceof Calendar) {
                        return NodeFactoryExtra.timeToNode((Calendar) value);
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired type");
                    }
                case Types.TINYINT:
                    if (value instanceof Byte) {
                        return LiteralFactory.byteToNode((Byte) value);
                    } else if (value instanceof Node) {
                        return LiteralFactory.byteToNode(JdbcNodeUtils.toByte((Node) value));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired type");
                    }
                default:
                    throw new SQLException("Cannot translate an unknown SQL Target Type into an appropriate RDF term type");
            }
        } catch (SQLException e) {
            // Throw as-is
            throw e;
        } catch (Throwable e) {
            // Wrap as SQL Exception
            throw new SQLException("Unexpected error trying to marshal a value to the desired target type", e);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object value, int targetSqlType, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object value, SQLType targetSqlType) throws SQLException {
        checkSupported("java.sql".equals(targetSqlType.getVendor()));  // see java.sql.JDBCType
        setObject(parameterIndex, value, targetSqlType.getVendorTypeNumber());
    }

    @Override
    public void setObject(int parameterIndex, Object value, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        checkSupported("java.sql".equals(targetSqlType.getVendor()));  // see java.sql.JDBCType
        setObject(parameterIndex, value, targetSqlType.getVendorTypeNumber(), scaleOrLength);
    }

    @Override
    public void setRef(int parameterIndex, Ref value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setShort(int parameterIndex, short value) throws SQLException {
        setParameter(parameterIndex, LiteralFactory.shortToNode(value));
    }

    @Override
    public void setString(int parameterIndex, String value) throws SQLException {
        setParameter(parameterIndex, value != null ? NodeFactory.createLiteral(value) : null);
    }

    @Override
    public void setTime(int parameterIndex, Time value) throws SQLException {
        setParameter(parameterIndex, value != null ? LiteralFactory.timeToNode(value) : null);
    }

    @Override
    public void setTime(int parameterIndex, Time value, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp value) throws SQLException {
        setParameter(parameterIndex, value != null ? LiteralFactory.timestampToNode(value) : null);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp value, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setURL(int parameterIndex, URL value) throws SQLException {
        setParameter(parameterIndex, value != null ? NodeFactory.createURI(value.toString()) : null);
    }

    @Deprecated
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream value, int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private void setParameter(int parameterIndex, Node n) throws SQLException {
        checkClosed();
        // Remember that JDBC uses a 1 based index
        check(parameterIndex >= 1 && parameterIndex <= paramMetadata.getParameterCount(), "Parameter Index is out of bounds");
        params.put("$data_world_param" + (parameterIndex - 1), n);
    }

    void setParameter(String parameterName, Node n) throws SQLException {
        checkClosed();
        check(parameterName != null && !parameterName.isEmpty(), "Empty or null parameter name");
        check(!parameterName.startsWith("data_world_param"), "May not set positional parameter values using named parameter methods");
        params.put("$" + parameterName, n);
    }

    @Override
    protected QueryEngineHTTP createQueryExecution(Query q) throws SQLException {
        QueryEngineHTTP execution = super.createQueryExecution(q);
        for (Map.Entry<String, Node> entry : params.entrySet()) {
            if (entry.getValue() != null) {
                execution.addParam(entry.getKey(), NTripleFormat.toString(entry.getValue()));
            }
        }
        return execution;
    }
}
