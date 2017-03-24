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
package world.data.jdbc.results;

import org.apache.jena.graph.Node;
import org.apache.jena.jdbc.utils.JdbcNodeUtils;
import world.data.jdbc.JdbcCompatibility;
import world.data.jdbc.statements.Statement;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.util.Conditions.check;

/**
 * Abstract implementation of a JDBC Result Set which makes all update methods
 * throw {@link SQLFeatureNotSupportedException}
 */
abstract class AbstractResultSet implements ResultSet {

    private final Statement statement;
    private final JdbcCompatibility compatibilityLevel;
    private SQLWarning warnings;
    private boolean wasNull;

    /**
     * Creates a new result set
     *
     * @param statement Statement that originated the result set
     * @throws SQLException Thrown if the arguments are invalid
     */
    AbstractResultSet(Statement statement) throws SQLException {
        this.statement = requireNonNull(statement, "statement");
        this.compatibilityLevel = statement.getJdbcCompatibilityLevel();
    }

    /**
     * Gets the JDBC compatibility level to use for the result set, this will
     * reflect the compatibility level at the time the result set was created
     * not necessarily the current compatibility level of the backing
     *
     * @return JDBC compatibility level, see {@link JdbcCompatibility}
     */
    public JdbcCompatibility getJdbcCompatibilityLevel() {
        return compatibilityLevel;
    }

    @Override
    public Statement getStatement() {
        return statement;
    }

    @Override
    public final void clearWarnings() {
        this.warnings = null;
    }

    @Override
    public final int getHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public final int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public abstract ResultSetMetaData getMetaData() throws SQLException;

    // Get Methods for things we do support

    /**
     * Helper method which derived classes must implement to map a column index
     * to a column label
     *
     * @param columnIndex Column Index
     * @return Column Label
     * @throws SQLException Should be thrown if the column index is invalid
     */
    protected abstract String findColumnLabel(int columnIndex) throws SQLException;

    /**
     * Helper method which derived classes must implement to retrieve the Node
     * for the given column of the current row
     *
     * @param columnLabel Column Label
     * @return Node if there is a value, null if no value for the column
     * @throws SQLException Should be thrown if there is no current row, the column label
     *                      is invalid or the result set is closed
     */
    protected abstract Node getNode(String columnLabel) throws SQLException;

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getBigDecimal(findColumnLabel(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return null;
        } else {
            // Try to marshal into a decimal
            setNull(false);
            return JdbcNodeUtils.toDecimal(n);
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getBoolean(findColumnLabel(columnIndex));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return false;
        } else {
            // Try to marshal into a boolean
            setNull(false);
            return JdbcNodeUtils.toBoolean(n);
        }
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getByte(findColumnLabel(columnIndex));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return 0;
        } else {
            // Try to marshal into a byte
            setNull(false);
            return JdbcNodeUtils.toByte(n);
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(findColumnLabel(columnIndex));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return null;
        } else {
            // Try to marshal into a date
            setNull(false);
            return JdbcNodeUtils.toDate(n);
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getDouble(findColumnLabel(columnIndex));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return 0;
        } else {
            // Try to marshal into a date
            setNull(false);
            return toDouble(n);
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getFloat(findColumnLabel(columnIndex));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return 0;
        } else {
            // Try to marshal into a date
            setNull(false);
            return toFloat(n);
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getInt(findColumnLabel(columnIndex));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return 0;
        } else {
            // Try to marshal into an integer
            setNull(false);
            return JdbcNodeUtils.toInt(n);
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getLong(findColumnLabel(columnIndex));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return 0;
        } else {
            // Try to marshal into an integer
            setNull(false);
            return JdbcNodeUtils.toLong(n);
        }
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getNString(findColumnLabel(columnIndex));
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return null;
        } else {
            setNull(false);
            return JdbcNodeUtils.toString(n);
        }
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObject(findColumnLabel(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return null;
        }
        // Need to marshal to an appropriate type based on declared JDBC
        // type of the column in order to comply with the JDBC semantics of
        // the getObject() method
        int jdbcType = getMetaData().getColumnType(findColumn(columnLabel));
        setNull(false);

        switch (jdbcType) {
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
                throw new SQLException("Unable to marhsal a RDF Node to the declared column type " + jdbcType);
            case Types.BOOLEAN:
                return JdbcNodeUtils.toBoolean(n);
            case Types.BIGINT:
                return JdbcNodeUtils.toLong(n);
            case Types.DATE:
                return JdbcNodeUtils.toDate(n);
            case Types.DECIMAL:
                return JdbcNodeUtils.toDecimal(n);
            case Types.DOUBLE:
                return JdbcNodeUtils.toDouble(n);
            case Types.FLOAT:
                return JdbcNodeUtils.toFloat(n);
            case Types.INTEGER:
                return JdbcNodeUtils.toInt(n);
            case Types.JAVA_OBJECT:
                return n;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
                return JdbcNodeUtils.toString(n);
            case Types.SMALLINT:
                return JdbcNodeUtils.toShort(n);
            case Types.TIME:
                return JdbcNodeUtils.toTime(n);
            case Types.TIMESTAMP:
                return JdbcNodeUtils.toTimestamp(n);
            case Types.TINYINT:
                return JdbcNodeUtils.toByte(n);
            default:
                throw new SQLException("Unable to marshal a RDF Node to the declared unknown column type " + jdbcType);
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getShort(findColumnLabel(columnIndex));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return 0;
        } else {
            // Try to marshal into an integer
            setNull(false);
            return JdbcNodeUtils.toShort(n);
        }
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return getString(findColumnLabel(columnIndex));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return null;
        } else {
            setNull(false);
            return JdbcNodeUtils.toString(n);
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(findColumnLabel(columnIndex));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return null;
        } else {
            // Try to marshal into a time
            setNull(false);
            return JdbcNodeUtils.toTime(n);
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(findColumnLabel(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return null;
        } else {
            // Try to marshal into a timestamp
            setNull(false);
            return JdbcNodeUtils.toTimestamp(n);
        }
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return getURL(findColumnLabel(columnIndex));
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        Node n = getNode(columnLabel);
        if (n == null) {
            setNull(true);
            return null;
        } else {
            setNull(false);
            return JdbcNodeUtils.toURL(n);
        }
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    /**
     * Helper method for setting the wasNull() status of the last column read
     *
     * @param wasNull Whether the last column was null
     */
    protected void setNull(boolean wasNull) {
        this.wasNull = wasNull;
    }

    //
    // Get Methods for things we don't support
    //

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getBigDecimal() is supported");
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getBigDecimal() is supported");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getDate() is supported");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getDate() is supported");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getObject() is supported");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getObject() is supported");
    }

    @Override
    @SuppressWarnings("javadoc")
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getObject() is supported");
    }

    @Override
    @SuppressWarnings("javadoc")
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getObject() is supported");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getTime() is supported");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getTime() is supported");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getTimestamp() is supported");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getTimestamp() is supported");
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final SQLWarning getWarnings() {
        return warnings;
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("data.world JDBC Result Sets are forward-only");
    }

    @Override
    public void refreshRow() {
        // No-op
    }

    @Override
    public boolean rowDeleted() {
        return false;
    }

    @Override
    public boolean rowInserted() {
        return false;
    }

    @Override
    public boolean rowUpdated() {
        return false;
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw newReadOnlyException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw newReadOnlyException();
    }

    private static double toDouble(Node n) throws SQLException {
        try {
            if (n == null) {
                return 0;
            }
            check(n.isLiteral(), "Unable to marshal a non-literal to an integer");
            return Double.parseDouble(n.getLiteralLexicalForm());
        } catch (SQLException e) {
            // Throw as is
            throw e;
        } catch (Exception e) {
            // Wrap other exceptions
            throw new SQLException("Unable to marshal the value to an integer", e);
        }
    }

    private static float toFloat(Node n) throws SQLException {
        try {
            if (n == null) {
                return 0;
            }
            check(n.isLiteral(), "Unable to marshal a non-literal to an integer");
            return Float.parseFloat(n.getLiteralLexicalForm());
        } catch (SQLException e) {
            // Throw as is
            throw e;
        } catch (Exception e) {
            // Wrap other exceptions
            throw new SQLException("Unable to marshal the value to an integer", e);
        }
    }

    private SQLFeatureNotSupportedException newReadOnlyException() {
        return new SQLFeatureNotSupportedException("data.world JDBC Result Sets are read-only");
    }
}
