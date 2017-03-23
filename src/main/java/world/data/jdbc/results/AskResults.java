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

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import world.data.jdbc.metadata.AskResultsMetadata;
import world.data.jdbc.statements.DataWorldStatement;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import static world.data.jdbc.util.Conditions.check;
import static world.data.jdbc.util.Conditions.checkSupported;

/**
 * Represents an ASK result
 */
public class AskResults extends AbstractResultsSet {

    private final boolean result;
    private final AskResultsMetadata metadata;
    private final String columnLabel;
    private int currRow;
    private boolean closed;

    /**
     * Creates a new ASK result
     *
     * @param statement Statement
     * @param result    Boolean result
     * @throws SQLException Thrown if the arguments are invalid
     */
    public AskResults(DataWorldStatement statement, boolean result) throws SQLException {
        super(statement);
        this.result = result;
        this.metadata = new AskResultsMetadata(this);
        this.columnLabel = metadata.getColumnLabel(AskResultsMetadata.COLUMN_INDEX_ASK);
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkClosed();
        // We can move backwards and forwards in an ASK result but there
        // is only ever a single row
        if (row == 1) {
            return first();
        } else if (row == -1) {
            return last();
        } else if (row == 0) {
            return true;
        } else {
            throw new SQLException("Moving the requested number of rows would be outside the allowable range of rows");
        }
    }

    @Override
    public void afterLast() {
        currRow = 2;
    }

    @Override
    public void beforeFirst() {
        currRow = 0;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        check(this.columnLabel.equals(columnLabel), "The given column does not exist in this result set");
        return 1;
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        currRow = 1;
        return true;
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public int getFetchSize() {
        return 1;
    }

    @Override
    public int getRow() {
        return currRow;
    }

    @Override
    public int getType() {
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return currRow == 2;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return currRow == 0;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return currRow == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        return currRow == 1;
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        currRow = 1;
        return true;
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (currRow < 2) {
            currRow++;
        }
        return currRow == 1;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClosed();
        if (currRow == 0 && (rows >= 0 && rows <= 2)) {
            currRow += rows;
            return true;
        } else if (currRow == 1 && (rows >= -1 && rows <= 1)) {
            currRow += rows;
            return true;
        } else if (currRow == 2 && (rows >= -2 && rows <= 0)) {
            currRow += rows;
            return true;
        } else {
            throw new SQLException("Moving the requested number of rows would be outside the allowable range of rows");
        }
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkSupported(direction == ResultSet.FETCH_FORWARD, "data.world JDBC Result Sets only support forward fetch");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("Fetch Size is not relevant for ASK results");
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return metadata;
    }

    @Override
    protected String findColumnLabel(int columnIndex) throws SQLException {
        check(columnIndex == AskResultsMetadata.COLUMN_INDEX_ASK, "Column Index is out of bounds");
        return columnLabel;
    }

    @Override
    protected Node getNode(String columnLabel) throws SQLException {
        checkClosed();
        check(currRow == 1, "Not currently at a row");
        check(this.columnLabel.equals(columnLabel), "The given column does not exist in the result set");
        return NodeFactory.createLiteral(Boolean.toString(result), XSDDatatype.XSDboolean);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        checkClosed();
        check(currRow == 1, "Not currently at a row");
        check(this.columnLabel.equals(columnLabel), "The given column does not exist in the result set");
        setNull(false);
        return result;
    }

    private void checkClosed() throws SQLException {
        check(!closed, "Result Set is closed");
    }
}
