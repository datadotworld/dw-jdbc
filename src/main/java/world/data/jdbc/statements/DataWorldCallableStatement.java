package world.data.jdbc.statements;

import org.apache.jena.atlas.web.auth.HttpAuthenticator;
import org.apache.jena.datatypes.xsd.XSDDatatype;
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
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;


public class DataWorldCallableStatement extends DataWorldPreparedStatement implements CallableStatement {
    protected Map<String, Node> namedParams = new HashMap<>();

    public DataWorldCallableStatement(final String query, final DataWorldConnection connection, final HttpAuthenticator authenticator, final QueryBuilder queryBuilder) throws SQLException {
        super(query, connection, authenticator, queryBuilder);
    }

    @Override
    public void setURL(final String parameterName, final URL value) throws SQLException {
        this.setParameter(parameterName, NodeFactory.createURI(value.toString()));

    }

    @Override
    public void setNull(final String parameterName, final int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters for statements are not nullable");
    }

    @Override
    public void setBoolean(final String parameterName, final boolean value) throws SQLException {
        this.setParameter(parameterName, NodeFactory.createLiteral(Boolean.toString(value), XSDDatatype.XSDboolean));
    }

    @Override
    public void setByte(final String parameterName, final byte value) throws SQLException {
        this.setParameter(parameterName, NodeFactory.createLiteral(Byte.toString(value), XSDDatatype.XSDbyte));
    }

    @Override
    public void setShort(final String parameterName, final short value) throws SQLException {
        this.setParameter(parameterName, NodeFactory.createLiteral(Short.toString(value), XSDDatatype.XSDshort));
    }

    @Override
    public void setInt(final String parameterName, final int value) throws SQLException {
        this.setParameter(parameterName, NodeFactoryExtra.intToNode(value));
    }

    @Override
    public void setLong(final String parameterName, final long value) throws SQLException {
        this.setParameter(parameterName, NodeFactoryExtra.intToNode(value));
    }

    @Override
    public void setFloat(final String parameterName, final float value) throws SQLException {
        this.setParameter(parameterName, NodeFactoryExtra.floatToNode(value));
    }

    @Override
    public void setDouble(final String parameterName, final double value) throws SQLException {
        this.setParameter(parameterName, NodeFactoryExtra.doubleToNode(value));
    }

    @Override
    public void setBigDecimal(final String parameterName, final BigDecimal value) throws SQLException {
        this.setParameter(parameterName, NodeFactory.createLiteral(value.toPlainString(), XSDDatatype.XSDdecimal));
    }

    @Override
    public void setString(final String parameterName, final String value) throws SQLException {
        this.setParameter(parameterName, NodeFactory.createLiteral(value));
    }

    @Override
    public void setBytes(final String parameterName, final byte[] value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(final String parameterName, final Date value) throws SQLException {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        c.setTimeInMillis(value.getTime());
        this.setParameter(parameterName, NodeFactoryExtra.dateTimeToNode(c));
    }

    @Override
    public void setTime(final String parameterName, final Time value) throws SQLException {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        c.setTimeInMillis(value.getTime());
        this.setParameter(parameterName, NodeFactoryExtra.timeToNode(c));
    }

    @Override
    public void setTimestamp(final String parameterName, final Timestamp value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(final String parameterName, final InputStream x, final int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(final String parameterName, final InputStream x, final int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(final String parameterName, final Object x, final int targetSqlType, final int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(final String parameterName, final Object value, final int targetSqlType) throws SQLException {
        if (value == null) throw new SQLException("Setting a null value is not permitted");

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
                        this.setParameter(parameterName, NodeFactoryExtra.intToNode((Long) value));
                    } else if (value instanceof Integer) {
                        this.setParameter(parameterName, NodeFactoryExtra.intToNode((long) (Integer) value));
                    } else if (value instanceof Short) {
                        this.setParameter(parameterName, NodeFactoryExtra.intToNode((long) (Short) value));
                    } else if (value instanceof Byte) {
                        this.setParameter(parameterName, NodeFactoryExtra.intToNode((long) (Byte) value));
                    } else if (value instanceof Node) {
                        long l = JdbcNodeUtils.toLong((Node) value);
                        this.setParameter(parameterName, NodeFactoryExtra.intToNode(l));
                    } else if (value instanceof String) {
                        this.setParameter(parameterName, NodeFactoryExtra.intToNode(Long.parseLong((String) value)));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                    break;
                case Types.BOOLEAN:
                    if (value instanceof Boolean) {
                        this.setParameter(parameterName,
                                NodeFactory.createLiteral(Boolean.toString((Boolean) value), XSDDatatype.XSDboolean));
                    } else if (value instanceof Node) {
                        boolean b = JdbcNodeUtils.toBoolean((Node) value);
                        this.setParameter(parameterName,
                                NodeFactory.createLiteral(Boolean.toString(b), XSDDatatype.XSDboolean));
                    } else if (value instanceof String) {
                        this.setParameter(parameterName, NodeFactory.createLiteral(Boolean.toString(Boolean.parseBoolean((String) value)), XSDDatatype.XSDboolean));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                    break;
                case Types.DATE:
                    if (value instanceof Date) {
                        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                        c.setTimeInMillis(((Date) value).getTime());
                        this.setParameter(parameterName, NodeFactoryExtra.dateTimeToNode(c));
                    } else if (value instanceof Node) {
                        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                        c.setTimeInMillis(JdbcNodeUtils.toDate((Node) value).getTime());
                        this.setParameter(parameterName, NodeFactoryExtra.dateTimeToNode(c));
                    } else if (value instanceof Time) {
                        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                        c.setTimeInMillis(((Time) value).getTime());
                        this.setParameter(parameterName, NodeFactoryExtra.timeToNode(c));
                    } else if (value instanceof Calendar) {
                        this.setParameter(parameterName, NodeFactoryExtra.dateTimeToNode((Calendar) value));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                    break;
                case Types.DECIMAL:
                    if (value instanceof BigDecimal) {
                        this.setParameter(parameterName, NodeFactory.createLiteral(((BigDecimal) value).toPlainString(), XSDDatatype.XSDdecimal));
                    } else if (value instanceof Node) {
                        BigDecimal d = JdbcNodeUtils.toDecimal((Node) value);
                        this.setParameter(parameterName, NodeFactory.createLiteral(d.toPlainString(), XSDDatatype.XSDdecimal));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                    break;
                case Types.DOUBLE:
                    if (value instanceof Double) {
                        this.setParameter(parameterName, NodeFactoryExtra.doubleToNode((Double) value));
                    } else if (value instanceof Float) {
                        this.setParameter(parameterName, NodeFactoryExtra.doubleToNode((Float) value));
                    } else if (value instanceof Node) {
                        Double d = JdbcNodeUtils.toDouble((Node) value);
                        this.setParameter(parameterName, NodeFactoryExtra.doubleToNode(d));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                    break;
                case Types.FLOAT:
                    if (value instanceof Float) {
                        this.setParameter(parameterName, NodeFactoryExtra.floatToNode((Float) value));
                    } else if (value instanceof Node) {
                        Float f = JdbcNodeUtils.toFloat((Node) value);
                        this.setParameter(parameterName, NodeFactoryExtra.floatToNode(f));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                    break;
                case Types.INTEGER:
                    if (value instanceof Integer) {
                        this.setParameter(parameterName, NodeFactoryExtra.intToNode((Integer) value));
                    } else if (value instanceof Short) {
                        this.setParameter(parameterName, NodeFactoryExtra.intToNode((Short) value));
                    } else if (value instanceof Byte) {
                        this.setParameter(parameterName, NodeFactoryExtra.intToNode((Byte) value));
                    } else if (value instanceof Node) {
                        Integer i = JdbcNodeUtils.toInt((Node) value);
                        this.setParameter(parameterName, NodeFactoryExtra.intToNode(i));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired target type");
                    }
                    break;
                case Types.JAVA_OBJECT:
                    if (value instanceof Node) {
                        this.setParameter(parameterName, (Node) value);
                    } else if (value instanceof RDFNode) {
                        this.setParameter(parameterName, ((RDFNode) value).asNode());
                    } else if (value instanceof String) {
                        this.setParameter(parameterName, NodeFactory.createLiteral((String) value));
                    } else if (value instanceof URL) {
                        this.setParameter(parameterName, NodeFactory.createURI(value.toString()));
                    } else if (value instanceof URI) {
                        this.setParameter(parameterName, NodeFactory.createURI(value.toString()));
                    } else if (value instanceof IRI) {
                        this.setParameter(parameterName, NodeFactory.createURI(value.toString()));
                    } else if (value instanceof BigDecimal) {
                        this.setParameter(parameterName, NodeFactory.createLiteral(((BigDecimal) value).toPlainString(), XSDDatatype.XSDdecimal));
                    } else if (value instanceof Boolean) {
                        this.setParameter(parameterName, NodeFactory.createLiteral(Boolean.toString(((Boolean) value)), XSDDatatype.XSDboolean));
                    } else if (value instanceof Byte) {
                        this.setParameter(parameterName, NodeFactory.createLiteral(Byte.toString((Byte) value), XSDDatatype.XSDbyte));
                    } else if (value instanceof Double) {
                        this.setParameter(parameterName, NodeFactoryExtra.doubleToNode((Double) value));
                    } else if (value instanceof Float) {
                        this.setParameter(parameterName, NodeFactoryExtra.floatToNode((Float) value));
                    } else if (value instanceof Short) {
                        this.setParameter(parameterName, NodeFactory.createLiteral(Short.toString((Short) value), XSDDatatype.XSDshort));
                    } else if (value instanceof Integer) {
                        this.setParameter(parameterName, NodeFactoryExtra.intToNode((Integer) value));
                    } else if (value instanceof Long) {
                        this.setParameter(parameterName, NodeFactoryExtra.intToNode((Long) value));
                    } else if (value instanceof Date) {
                        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                        c.setTimeInMillis(((Date) value).getTime());
                        this.setParameter(parameterName, NodeFactoryExtra.dateTimeToNode(c));
                    } else if (value instanceof Time) {
                        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                        c.setTimeInMillis(((Time) value).getTime());
                        this.setParameter(parameterName, NodeFactoryExtra.timeToNode(c));
                    } else if (value instanceof Calendar) {
                        this.setParameter(parameterName, NodeFactoryExtra.dateTimeToNode((Calendar) value));
                    } else {
                        this.setParameter(parameterName, NodeFactory.createLiteral(value.toString()));
                    }
                    break;
                case Types.CHAR:
                case Types.NVARCHAR:
                case Types.VARCHAR:
                    this.setParameter(parameterName, NodeFactory.createLiteral(value.toString()));
                    break;
                case Types.SMALLINT:
                    if (value instanceof Short) {
                        Short s = (Short) value;
                        this.setParameter(parameterName, NodeFactory.createLiteral(Short.toString(s), XSDDatatype.XSDshort));
                    } else if (value instanceof Byte) {
                        this.setParameter(parameterName, NodeFactory.createLiteral(Short.toString((Byte) value), XSDDatatype.XSDshort));
                    } else if (value instanceof Node) {
                        Short s = JdbcNodeUtils.toShort((Node) value);
                        this.setParameter(parameterName, NodeFactory.createLiteral(Short.toString(s), XSDDatatype.XSDshort));
                    } else if (value instanceof String) {
                        this.setParameter(parameterName, NodeFactory.createLiteral(Short.toString(Short.parseShort((String) value)), XSDDatatype.XSDshort));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired type");
                    }
                    break;
                case Types.TIME:
                    if (value instanceof Time) {
                        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                        c.setTimeInMillis(((Time) value).getTime());
                        this.setParameter(parameterName, NodeFactoryExtra.timeToNode(c));
                    } else if (value instanceof Node) {
                        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                        c.setTimeInMillis(JdbcNodeUtils.toDate((Node) value).getTime());
                        this.setParameter(parameterName, NodeFactoryExtra.timeToNode(c));
                    } else if (value instanceof Date) {
                        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                        c.setTimeInMillis(((Date) value).getTime());
                        this.setParameter(parameterName, NodeFactoryExtra.timeToNode(c));
                    } else if (value instanceof Calendar) {
                        this.setParameter(parameterName, NodeFactoryExtra.timeToNode((Calendar) value));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired type");
                    }
                    break;
                case Types.TINYINT:
                    if (value instanceof Byte) {
                        Byte b = (Byte) value;
                        this.setParameter(parameterName, NodeFactory.createLiteral(Byte.toString(b), XSDDatatype.XSDbyte));
                    } else if (value instanceof Node) {
                        Byte b = JdbcNodeUtils.toByte((Node) value);
                        this.setParameter(parameterName, NodeFactory.createLiteral(Byte.toString(b), XSDDatatype.XSDbyte));
                    } else {
                        throw new SQLException("The given value is not marshallable to the desired type");
                    }
                    break;
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
    public void setObject(final String parameterName, final Object value) throws SQLException {
        if (value == null) throw new SQLException("Setting a null value is not permitted");

        if (value instanceof Node) {
            this.setParameter(parameterName, (Node) value);
        } else if (value instanceof RDFNode) {
            this.setParameter(parameterName, ((RDFNode) value).asNode());
        } else if (value instanceof String) {
            this.setParameter(parameterName, NodeFactory.createLiteral((String) value));
        } else if (value instanceof Boolean) {
            this.setParameter(parameterName,
                    NodeFactory.createLiteral(Boolean.toString((Boolean) value), XSDDatatype.XSDboolean));
        } else if (value instanceof Long) {
            this.setParameter(parameterName, NodeFactoryExtra.intToNode((Long) value));
        } else if (value instanceof Integer) {
            this.setParameter(parameterName, NodeFactoryExtra.intToNode((Integer) value));
        } else if (value instanceof Short) {
            this.setParameter(parameterName, NodeFactory.createLiteral(Short.toString((Short) value), XSDDatatype.XSDshort));
        } else if (value instanceof Byte) {
            this.setParameter(parameterName, NodeFactory.createLiteral(Byte.toString((Byte) value), XSDDatatype.XSDbyte));
        } else if (value instanceof BigDecimal) {
            this.setParameter(parameterName,
                    NodeFactory.createLiteral(((BigDecimal) value).toPlainString(), XSDDatatype.XSDdecimal));
        } else if (value instanceof Float) {
            this.setParameter(parameterName, NodeFactoryExtra.floatToNode((Float) value));
        } else if (value instanceof Double) {
            this.setParameter(parameterName, NodeFactoryExtra.doubleToNode((Double) value));
        } else if (value instanceof Date) {
            Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            c.setTimeInMillis(((Date) value).getTime());
            this.setParameter(parameterName, NodeFactoryExtra.dateTimeToNode(c));
        } else if (value instanceof Time) {
            Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            c.setTimeInMillis(((Time) value).getTime());
            this.setParameter(parameterName, NodeFactoryExtra.timeToNode(c));
        } else if (value instanceof Calendar) {
            this.setParameter(parameterName, NodeFactoryExtra.dateTimeToNode((Calendar) value));
        } else if (value instanceof URL) {
            this.setParameter(parameterName, NodeFactory.createURI(value.toString()));
        } else if (value instanceof URI) {
            this.setParameter(parameterName, NodeFactory.createURI(value.toString()));
        } else if (value instanceof IRI) {
            this.setParameter(parameterName, NodeFactory.createURI(value.toString()));
        } else {
            throw new SQLException(
                    "setObject() received a value that could not be converted to a RDF node for use in a SPARQL query");
        }    }

    @Override
    public void setCharacterStream(final String parameterName, final Reader reader, final int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(final String parameterName, final Date x, final Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTime(final String parameterName, final Time x, final Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTimestamp(final String parameterName, final Timestamp x, final Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNull(final String parameterName, final int sqlType, final String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(final String parameterName, final RowId value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(final String parameterName, final String value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(final String parameterName, final Reader value, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(final String parameterName, final NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(final String parameterName, final Reader reader, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(final String parameterName, final InputStream inputStream, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(final String parameterName, final Reader reader, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(final String parameterName, final SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(final String parameterName, final Blob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(final String parameterName, final Clob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(final String parameterName, final InputStream x, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(final String parameterName, final InputStream x, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(final String parameterName, final Reader reader, final long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(final String parameterName, final InputStream value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(final String parameterName, final InputStream value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(final String parameterName, final Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(final String parameterName, final Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(final String parameterName, final Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(final String parameterName, final InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(final String parameterName, final Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public String getString(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public boolean getBoolean(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public byte getByte(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public short getShort(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public int getInt(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public long getLong(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public float getFloat(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public double getDouble(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public byte[] getBytes(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Date getDate(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Time getTime(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Timestamp getTimestamp(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Object getObject(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public BigDecimal getBigDecimal(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Object getObject(final String parameterName, final Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Ref getRef(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Blob getBlob(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Clob getClob(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Array getArray(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Date getDate(final String parameterName, final Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Time getTime(final String parameterName, final Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Timestamp getTimestamp(final String parameterName, final Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public URL getURL(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public RowId getRowId(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public RowId getRowId(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public NClob getNClob(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public NClob getNClob(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public SQLXML getSQLXML(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public SQLXML getSQLXML(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public String getNString(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public String getNString(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Reader getNCharacterStream(final int parameterName) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Reader getCharacterStream(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Reader getCharacterStream(final String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public <T> T getObject(final int parameterName, final Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public String getString(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public boolean getBoolean(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public byte getByte(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public short getShort(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public int getInt(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public long getLong(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public float getFloat(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public double getDouble(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public BigDecimal getBigDecimal(final int parameterName, final int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public byte[] getBytes(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Date getDate(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Time getTime(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Timestamp getTimestamp(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Object getObject(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public BigDecimal getBigDecimal(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Object getObject(final int parameterName, final Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Ref getRef(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Blob getBlob(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Clob getClob(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Array getArray(final int parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Date getDate(final int parameterName, final Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Time getTime(final int parameterName, final Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public Timestamp getTimestamp(final int parameterName, final Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public <T> T getObject(final String parameterName, final Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void setObject(final String parameterName, final Object x, final SQLType targetSqlType, final int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void setObject(final String parameterName, final Object x, final SQLType targetSqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void registerOutParameter(final int parameterName, final SQLType sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void registerOutParameter(final int parameterName, final SQLType sqlType, final int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void registerOutParameter(final int parameterName, final SQLType sqlType, final String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void registerOutParameter(final String parameterName, final SQLType sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void registerOutParameter(final String parameterName, final SQLType sqlType, final int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void registerOutParameter(final String parameterName, final SQLType sqlType, final String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void registerOutParameter(final int parameterName, final int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void registerOutParameter(final int parameterName, final int sqlType, final int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void registerOutParameter(final int parameterName, final int sqlType, final String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void registerOutParameter(final String parameterName, final int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void registerOutParameter(final String parameterName, final int sqlType, final int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public void registerOutParameter(final String parameterName, final int sqlType, final String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    @Override
    public boolean wasNull() throws SQLException {
        throw new SQLFeatureNotSupportedException("Output parameters not supported");
    }

    private void setParameter(String parameterName, Node n) throws SQLException {
        namedParams.put(parameterName, n);
    }

    @Override
    protected QueryEngineHTTP createQueryExecution(Query q) throws SQLException {
        final QueryEngineHTTP execution = super.createQueryExecution(q);
        if (!params.isEmpty() || !namedParams.isEmpty()) {
            execution.addParam("parameters", formatParams());
        }
        return execution;
    }
    protected String formatParams() {
        final StringBuilder out = new StringBuilder();
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
