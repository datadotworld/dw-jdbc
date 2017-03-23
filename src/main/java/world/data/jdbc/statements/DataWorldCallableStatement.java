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
import org.apache.jena.query.Query;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import world.data.jdbc.connections.DataWorldConnection;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;


public class DataWorldCallableStatement extends DataWorldPreparedStatement implements ReadOnlyCallableStatement {
    private final Map<String, Node> namedParams = new HashMap<>();

    public DataWorldCallableStatement(String query, DataWorldConnection connection, QueryBuilder queryBuilder) throws SQLException {
        super(query, connection, queryBuilder);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream value, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal value) throws SQLException {
        setParameter(parameterName, LiteralFactory.bigDecimalToNode(value));
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream value, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(String parameterName, Blob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(String parameterName, InputStream value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(String parameterName, InputStream value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBoolean(String parameterName, boolean value) throws SQLException {
        setParameter(parameterName, LiteralFactory.booleanToNode(value));
    }

    @Override
    public void setByte(String parameterName, byte value) throws SQLException {
        setParameter(parameterName, LiteralFactory.byteToNode(value));
    }

    @Override
    public void setBytes(String parameterName, byte[] value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(String parameterName, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(String parameterName, Reader value, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(String parameterName, Clob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(String parameterName, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(String parameterName, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(String parameterName, Date value) throws SQLException {
        setParameter(parameterName, LiteralFactory.dateTimeToNode(value));
    }

    @Override
    public void setDate(String parameterName, Date value, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDouble(String parameterName, double value) throws SQLException {
        setParameter(parameterName, LiteralFactory.doubleToNode(value));
    }

    @Override
    public void setFloat(String parameterName, float value) throws SQLException {
        setParameter(parameterName, LiteralFactory.floatToNode(value));
    }

    @Override
    public void setInt(String parameterName, int value) throws SQLException {
        setParameter(parameterName, NodeFactoryExtra.intToNode(value));
    }

    @Override
    public void setLong(String parameterName, long value) throws SQLException {
        setParameter(parameterName, NodeFactoryExtra.intToNode(value));
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        setParameter(parameterName, NodeFactory.createLiteral(value));
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters for statements are not nullable");
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters for statements are not nullable");
    }

    @Override
    public void setObject(String parameterName, Object value) throws SQLException {
        setParameter(parameterName, objectToNode(value));
    }

    @Override
    public void setObject(String parameterName, Object value, int targetSqlType) throws SQLException {
        setParameter(parameterName, objectToNode(value, targetSqlType));
    }

    @Override
    public void setObject(String parameterName, Object value, int targetSqlType, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(String parameterName, Object value, SQLType targetSqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(String parameterName, Object value, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(String parameterName, RowId value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setShort(String parameterName, short value) throws SQLException {
        setParameter(parameterName, LiteralFactory.shortToNode(value));
    }

    @Override
    public void setString(String parameterName, String value) throws SQLException {
        setParameter(parameterName, NodeFactory.createLiteral(value));
    }

    @Override
    public void setTime(String parameterName, Time value) throws SQLException {
        setParameter(parameterName, LiteralFactory.timeToNode(value));
    }

    @Override
    public void setTime(String parameterName, Time value, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp value, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setURL(String parameterName, URL value) throws SQLException {
        setParameter(parameterName, NodeFactory.createURI(value.toString()));
    }

    private void setParameter(String parameterName, Node n) throws SQLException {
        namedParams.put(parameterName, n);
    }

    @Override
    protected QueryEngineHTTP createQueryExecution(Query q) throws SQLException {
        QueryEngineHTTP execution = super.createQueryExecution(q);
        if (!params.isEmpty() || !namedParams.isEmpty()) {
            execution.addParam("parameters", formatParams());
        }
        return execution;
    }

    protected String formatParams() {
        StringBuilder out = new StringBuilder();
        boolean first = true;
        for (Map.Entry<Integer, Node> param : params.entrySet()) {
            if (first) {
                first = false;
            } else {
                out.append(",");
            }
            out.append("$data_world_param");
            out.append(param.getKey() - 1);
            out.append("=");
            out.append(normalizeValue(param.getValue().toString()));
        }
        for (Map.Entry<String, Node> param : namedParams.entrySet()) {
            if (first) {
                first = false;
            } else {
                out.append(",");
            }
            out.append(param.getKey());
            out.append("=");
            out.append(normalizeValue(param.getValue().toString()));
        }
        return out.toString();
    }
}
