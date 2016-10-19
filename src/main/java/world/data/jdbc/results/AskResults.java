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

/**
 * Represents an ASK result
 *
 */
public class AskResults extends DataWorldResultsSet {

    private boolean result, closed = false;
    private int currRow = 0;
    private AskResultsMetadata metadata;
    private String columnLabel;

    /**
     * Creates a new ASK result
     * @param statement Statement
     * @param result Boolean result
     * @throws SQLException Thrown if the arguments are invalid
     */
    public AskResults(DataWorldStatement statement, boolean result) throws SQLException {
        super(statement);
        this.result = result;
        this.metadata = new AskResultsMetadata(this);
        this.columnLabel = this.metadata.getColumnLabel(AskResultsMetadata.COLUMN_INDEX_ASK);
    }

    public boolean absolute(int row) throws SQLException {
        // We can move backwards and forwards in an ASK result but there
        // is only ever a single row
        if (this.isClosed()) {
            throw new SQLException("Cannot move to a row after the result set has been closed");
        } else if (row == 1) {
            return this.first();
        } else if (row == -1) {
            return this.last();
        } else if (row == 0) {
            return true;
        } else {
            throw new SQLException("Moving the requested number of rows would be outside the allowable range of rows");
        }
    }

    public void afterLast() {
        this.currRow = 2;
    }

    public void beforeFirst() {
        this.currRow = 0;
    }

    public void close() throws SQLException {
        if (this.closed) return;
        this.closed = true;
    }

    public int findColumn(String columnLabel) throws SQLException {
        if (this.columnLabel.equals(columnLabel)) return 1;
        throw new SQLException("The given column does not exist in this result set");
    }

    public boolean first() throws SQLException {
        if (this.isClosed()) throw new SQLException("Cannot move to a row after the result set has been closed");
        this.currRow = 1;
        return true;
    }

    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    public int getFetchSize() {
        return 1;
    }

    public int getRow() {
        return this.currRow;
    }

    public int getType() {
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    public boolean isAfterLast() throws SQLException {
        if (this.isClosed()) throw new SQLException("Result Set is closed");
        return this.currRow == 2;
    }

    public boolean isBeforeFirst() throws SQLException {
        if (this.isClosed()) throw new SQLException("Result Set is closed");
        return this.currRow == 0;
    }

    public boolean isClosed() {
        return this.closed;
    }

    public boolean isFirst() throws SQLException {
        if (this.isClosed()) throw new SQLException("Result Set is closed");
        return this.currRow == 1;
    }

    public boolean isLast() throws SQLException {
        if (this.isClosed()) throw new SQLException("Result Set is closed");
        return this.currRow == 1;
    }

    public boolean last() throws SQLException {
        if (this.isClosed()) throw new SQLException("Cannot move to a row after the result set has been closed");
        this.currRow = 1;
        return true;
    }

    public boolean next() throws SQLException {
        if (this.isClosed()) throw new SQLException("Cannot move to a row after the result set has been closed");
        if (this.currRow < 2) {
            this.currRow++;
        }
        return this.currRow == 1;
    }

    public boolean relative(int rows) throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Cannot move to a row after the result set has been closed");
        } else if (this.currRow == 0 && (rows >= 0 && rows <= 2)) {
            this.currRow += rows;
            return true;
        } else if (this.currRow == 1 && (rows >= -1 && rows <= 1)) {
            this.currRow += rows;
            return true;
        } else if (this.currRow == 2 && (rows >= -2 && rows <= 0)) {
            this.currRow += rows;
            return true;
        } else {
            throw new SQLException("Moving the requested number of rows would be outside the allowable range of rows");
        }
    }

    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) throw new SQLFeatureNotSupportedException("Jena JDBC Result Sets only support forward fetch");
    }

    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("Fetch Size is not relevant for ASK results");
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return this.metadata;
    }

    @Override
    protected String findColumnLabel(int columnIndex) throws SQLException {
        if (columnIndex == AskResultsMetadata.COLUMN_INDEX_ASK) return this.columnLabel;
        throw new SQLException("Column Index is out of bounds");
    }

    @Override
    protected Node getNode(String columnLabel) throws SQLException {
        if (this.isClosed()) throw new SQLException("Result Set is closed");
        if (this.currRow != 1) throw new SQLException("Not currently at a row");
        if (this.columnLabel.equals(columnLabel)) {
            return NodeFactory.createLiteral(Boolean.toString(this.result), XSDDatatype.XSDboolean);
        } else {
            throw new SQLException("The given column does not exist in the result set");
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        if (this.isClosed()) throw new SQLException("Result Set is closed");
        if (this.currRow != 1) throw new SQLException("Not currently at a row");
        if (this.columnLabel.equals(columnLabel)) {
            this.setNull(false);
            return this.result;
        } else {
            throw new SQLException("The given column does not exist in the result set");
        }
    }


}
