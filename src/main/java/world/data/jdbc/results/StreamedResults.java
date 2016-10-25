package world.data.jdbc.results;

import org.apache.jena.query.QueryExecution;
import world.data.jdbc.statements.DataWorldStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Represents a set of streamed results backed by some {@link QueryExecution},
 * streamed results are considered to be forward only
 *
 * @param <T> Type of the underlying result rows
 */
abstract class StreamedResults<T> extends QueryExecutionResults {

    private T currItem;
    private boolean finished = false;
    private int currRow = 0;

    /**
     * Creates new streamed results
     *
     * @param statement Statement that created the result set
     * @param qe        Query Execution
     * @throws SQLException Thrown if the arguments are invalid
     */
    StreamedResults(DataWorldStatement statement, QueryExecution qe) throws SQLException {
        super(statement, qe);
    }

    /**
     * Gets the current result row (if any)
     *
     * @return Result row, null if not at a row
     * @throws SQLException Thrown if the result set is closed
     */
    T getCurrentRow() throws SQLException {
        if (this.isClosed())
            throw new SQLException("Result set is closed");
        return this.currItem;
    }

    /**
     * Method which derived classes must implement to indicate whether they have
     * further rows available
     *
     * @return True if further rows are available, false otherwise
     * @throws SQLException Thrown if an error determining whether rows are available
     */
    protected abstract boolean hasNext() throws SQLException;

    /**
     * Method which derived classes must implement to provide the next row
     * available
     *
     * @return Next row available
     * @throws SQLException Thrown if this method is invoked when no further rows are
     *                      available
     */
    protected abstract T moveNext() throws SQLException;

    @Override
    public final boolean absolute(int row) throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Cannot move to a row after the result set has been closed");
        } else if (row == 1) {
            // Try and move to the first row
            return this.first();
        } else if (row == -1) {
            // Try and move to the last row
            return this.last();
        } else if (row <= 0) {
            // Can't move to an arbitrary relative row from the end of the
            // results
            throw new SQLException(
                    "data.world JDBC result sets are forward only, cannot move to a row which is relative to the end of the result set since the number of result rows is not known in advance");
        } else if (row == this.currRow) {
            // Already at the desired row
            return true;
        } else if (row < this.currRow) {
            throw new SQLException("data.world JDBC result sets are forward only, cannot move backwards");
        } else {
            // Before the desired row
            while (this.hasNext() && this.currRow < row) {
                this.currItem = this.moveNext();
                this.currRow++;
            }
            // If we didn't reach it we hit the end of the result set
            if (this.currRow < row) {
                this.finished = true;
                this.currItem = null;
            }
            return (row == this.currRow);
        }
    }

    @Override
    public final void afterLast() throws SQLException {
        if (this.isClosed())
            throw new SQLException("Result Set is closed");
        if (finished)
            return;

        // Move to end of results
        while (this.hasNext()) {
            this.currItem = this.moveNext();
            this.currRow++;
        }
        this.currItem = null;
        this.finished = true;
    }

    @Override
    public final void beforeFirst() throws SQLException {
        if (this.isClosed())
            throw new SQLException("Result Set is closed");
        // If we've started throw an error as we can't move backwards
        if (this.currRow > 0)
            throw new SQLException(
                    "data.world JDBC result sets are forward only, can't move to before the start of the result set after navigation through the result set has begun");
        // Otherwise OK
        this.currItem = null;
    }

    @Override
    protected final void closeInternal() throws SQLException {
        this.currItem = null;
        this.finished = true;
        this.closeStreamInternal();
    }

    protected abstract void closeStreamInternal() throws SQLException;

    @Override
    public final boolean first() throws SQLException {
        if (this.isClosed())
            throw new SQLException("Result Set is closed");
        if (this.currRow == 1)
            return true;
        throw new SQLException(
                "data.world JDBC result sets are forward only, can't move backwards to the first row after the first row has been passed");
    }

    @Override
    public final int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public final int getFetchSize() {
        return 0;
    }

    @Override
    public final int getRow() {
        return this.currRow;
    }

    @Override
    public final int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public final boolean isAfterLast() throws SQLException {
        if (this.isClosed())
            throw new SQLException("Result Set is closed");
        return this.finished;
    }

    @Override
    public final boolean isBeforeFirst() throws SQLException {
        if (this.isClosed())
            throw new SQLException("Result Set is closed");
        return this.currRow == 0;
    }

    @Override
    public final boolean isFirst() throws SQLException {
        if (this.isClosed())
            throw new SQLException("Result Set is closed");
        return this.currRow == 1;
    }

    @Override
    public final boolean isLast() throws SQLException {
        if (this.isClosed())
            throw new SQLException("Result Set is closed");
        return !this.hasNext();
    }

    @Override
    public final boolean last() throws SQLException {
        if (this.isClosed() || this.finished) {
            throw new SQLException("data.world JDBC Result Sets are forward-only");
        } else {
            while (this.hasNext()) {
                this.currItem = this.moveNext();
                this.currRow++;
            }
            return true;
        }
    }

    @Override
    public final boolean next() throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Cannot move to the next row in a closed result set");
        } else {
            if (this.hasNext()) {
                this.currItem = this.moveNext();
                this.currRow++;
                return true;
            } else {
                if (!this.finished)
                    this.currRow++;
                this.finished = true;
                return false;
            }
        }
    }

    @Override
    public final boolean relative(int rows) throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Cannot move to a row after the result set has been closed");
        } else if (rows == 0) {
            // Already at the desired row
            return true;
        } else if (rows < 0) {
            throw new SQLException("data.world JDBC result sets are forward only, cannot move backwards");
        } else {
            // Before the desired row
            int moved = 0;
            while (this.hasNext() && moved < rows) {
                this.currItem = this.moveNext();
                this.currRow++;
                moved++;
            }
            // If we didn't reach it we hit the end of the result set
            if (moved < rows) {
                this.finished = true;
                this.currItem = null;
            }
            return (rows == moved);
        }
    }

    @Override
    public final void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD)
            throw new SQLFeatureNotSupportedException("data.world JDBC Result Sets only support forward fetch");
    }

    @Override
    public final void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}