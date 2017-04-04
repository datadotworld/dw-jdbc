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
package world.data.jdbc.internal.results;

import world.data.jdbc.DataWorldStatement;
import world.data.jdbc.internal.types.NodeConversions;
import world.data.jdbc.internal.types.NodeValues;
import world.data.jdbc.internal.util.ResourceContainer;
import world.data.jdbc.model.Node;

import javax.annotation.Nullable;
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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.internal.util.Conditions.check;

/**
 * A forward-only result set backed by an iterator of rows of RDF nodes.
 */
public final class ResultSetImpl implements ResultSet, ReadOnlyResultSet, ForwardOnlyResultSet, IndexBasedResultSet {
    private final DataWorldStatement statement;
    private final ResultSetMetaData metaData;
    private final Iterator<Node[]> rowIter;
    private AutoCloseable cleanup;
    private final Map<String, Integer> columnIndexByLabel;
    private SQLWarning warnings;
    private Node[] row;
    private boolean wasNull;
    private boolean closed;

    /**
     * Creates a non-streaming forward-only result set.
     */
    public ResultSetImpl(@Nullable DataWorldStatement statement, ResultSetMetaData metaData, List<Node[]> rows)
            throws SQLException {
        this(statement, metaData, rows.iterator(), null);
    }

    /**
     * Creates a streaming forward-only result set that invokes the specified cleanup function on close or when
     * the client reaches the end of the result set.
     */
    public ResultSetImpl(@Nullable DataWorldStatement statement, ResultSetMetaData metaData, Iterator<Node[]> rowIter,
                         @Nullable AutoCloseable cleanup)
            throws SQLException {
        this.statement = statement;
        this.metaData = requireNonNull(metaData, "metaData");
        this.rowIter = requireNonNull(rowIter, "rowIter");
        this.cleanup = cleanup;
        if (statement != null) {
            ((ResourceContainer) statement).getResources().register(this);
        }

        // Index the columns by label, for findColumn(String columnLabel)
        Map<String, Integer> columnIndexByLabel = new LinkedHashMap<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            columnIndexByLabel.put(metaData.getColumnLabel(i), i);
        }
        this.columnIndexByLabel = columnIndexByLabel;
    }

    @Override
    public DataWorldStatement getStatement() throws SQLException {
        checkClosed();
        return statement;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return metaData;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        warnings = null;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        check(rows >= 0, "Fetch size must be non-negative");
        // The fetch size is a hint that this class ignores
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        try {
            cleanup();
        } finally {
            closed = true;
        }
    }

    private void cleanup() throws SQLException {
        if (statement != null) {
            ((ResourceContainer) statement).getResources().remove(this);
        }
        if (cleanup != null) {
            try {
                cleanup.close();
            } catch (SQLException e) {
                throw e;
            } catch (Exception e) {
                throw new SQLException("Unexpected error closing the result set", e);
            } finally {
                cleanup = null;
            }
        }
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (rowIter.hasNext()) {
            row = rowIter.next();
            return true;
        } else {
            // No more rows, go ahead & release any resources we might be holding
            cleanup();
            row = null;
            return false;
        }
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        Integer index = columnIndexByLabel.get(columnLabel);
        check(index != null, "The given column does not exist in the result set");
        return index;
    }

    /** Returns the value for a specific column, setting {@link #wasNull()} as a side-effect. */
    @Nullable
    private Node getNode(int columnIndex) throws SQLException {
        checkClosed();
        check(row != null, "Not currently at a row");
        check(columnIndex >= 1 && columnIndex <= row.length, "Column index out-of-bounds");
        Node node = row[columnIndex - 1];
        wasNull = (node == null);
        return node;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return wasNull;
    }

    private void checkClosed() throws SQLException {
        check(!closed, "Result Set is closed");
    }

    //
    // Column value getter functions
    //

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return NodeValues.parseBigDecimal(getNode(columnIndex));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return NodeValues.parseBoolean(getNode(columnIndex), false);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return NodeValues.parseByte(getNode(columnIndex), (byte) 0);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return Date.valueOf(NodeValues.parseLocalDate(getNode(columnIndex)));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return NodeValues.parseDouble(getNode(columnIndex), 0d);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return NodeValues.parseFloat(getNode(columnIndex), 0f);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return NodeValues.parseInteger(getNode(columnIndex), 0);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return NodeValues.parseLong(getNode(columnIndex), 0L);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return NodeConversions.toString(getNode(columnIndex));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        int jdbcType = getMetaData().getColumnType(columnIndex);
        return NodeConversions.toObject(getNode(columnIndex), jdbcType);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        check(type != null, "Type argument may not be null");
        return NodeConversions.toObject(getNode(columnIndex), type);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return NodeValues.parseShort(getNode(columnIndex), (short) 0);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return NodeConversions.toString(getNode(columnIndex));
    }

    @Override
    @SuppressWarnings("deprecation")
    public Time getTime(int columnIndex) throws SQLException {
        return Time.valueOf(LocalTime.from(NodeValues.parseBestTime(getNode(columnIndex))));
    }

    @Override
    @SuppressWarnings("deprecation")
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return Timestamp.valueOf(LocalDateTime.from(NodeValues.parseBestDateTime(getNode(columnIndex))));
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return NodeValues.parseUrl(getNode(columnIndex));
    }

    //
    // Methods for things we don't support
    //

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
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getBigDecimal() is supported");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
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
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getDate() is supported");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getObject() is supported");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getTime() is supported");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only the single argument form of getTimestamp() is supported");
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
