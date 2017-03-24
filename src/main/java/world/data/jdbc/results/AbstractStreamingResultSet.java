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

import org.apache.jena.query.QueryExecution;
import world.data.jdbc.statements.Statement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import static world.data.jdbc.util.Conditions.check;
import static world.data.jdbc.util.Conditions.checkSupported;

/**
 * Represents a set of streamed results backed by some {@link QueryExecution},
 * streamed results are considered to be forward only
 *
 * @param <T> Type of the underlying result rows
 */
abstract class AbstractStreamingResultSet<T> extends AbstractQueryExecutionResults {

    private T currItem;
    private int currRow;
    private boolean finished;

    /**
     * Creates new streamed results
     *
     * @param statement Statement that created the result set
     * @param qe        Query Execution
     * @throws SQLException Thrown if the arguments are invalid
     */
    AbstractStreamingResultSet(Statement statement, QueryExecution qe) throws SQLException {
        super(statement, qe);
    }

    /**
     * Gets the current result row (if any)
     *
     * @return Result row, null if not at a row
     * @throws SQLException Thrown if the result set is closed
     */
    T getCurrentRow() throws SQLException {
        checkClosed();
        return currItem;
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
        checkClosed();
        if (row == 1) {
            // Try and move to the first row
            return first();
        } else if (row == -1) {
            // Try and move to the last row
            return last();
        } else if (row <= 0) {
            // Can't move to an arbitrary relative row from the end of the results
            throw new SQLException("data.world JDBC result sets are forward only, cannot move to a row which is relative to the end of the result set since the number of result rows is not known in advance");
        } else if (row == currRow) {
            // Already at the desired row
            return true;
        } else if (row < currRow) {
            throw new SQLException("data.world JDBC result sets are forward only, cannot move backwards");
        } else {
            // Before the desired row
            while (hasNext() && currRow < row) {
                currItem = moveNext();
                currRow++;
            }
            // If we didn't reach it we hit the end of the result set
            if (currRow < row) {
                finished = true;
                currItem = null;
            }
            return (row == currRow);
        }
    }

    @Override
    public final void afterLast() throws SQLException {
        checkClosed();
        if (finished) {
            return;
        }
        // Move to end of results
        while (hasNext()) {
            currItem = moveNext();
            currRow++;
        }
        currItem = null;
        finished = true;
    }

    @Override
    public final void beforeFirst() throws SQLException {
        checkClosed();
        // If we've started throw an error as we can't move backwards
        check(currRow == 0, "data.world JDBC result sets are forward only, can't move to before the start of the result set after navigation through the result set has begun");
        // Otherwise OK
        currItem = null;
    }

    @Override
    protected final void closeInternal() throws SQLException {
        currItem = null;
        finished = true;
        closeStreamInternal();
    }

    protected abstract void closeStreamInternal() throws SQLException;

    @Override
    public final boolean first() throws SQLException {
        checkClosed();
        check(currRow == 1, "data.world JDBC result sets are forward only, can't move backwards to the first row after the first row has been passed");
        return true;
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
        return currRow;
    }

    @Override
    public final int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public final boolean isAfterLast() throws SQLException {
        checkClosed();
        return finished;
    }

    @Override
    public final boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return currRow == 0;
    }

    @Override
    public final boolean isFirst() throws SQLException {
        checkClosed();
        return currRow == 1;
    }

    @Override
    public final boolean isLast() throws SQLException {
        checkClosed();
        return !hasNext();
    }

    @Override
    public final boolean last() throws SQLException {
        checkClosed();
        check(!finished, "data.world JDBC Result Sets are forward-only");
        while (hasNext()) {
            currItem = moveNext();
            currRow++;
        }
        return true;
    }

    @Override
    public final boolean next() throws SQLException {
        checkClosed();
        if (hasNext()) {
            currItem = moveNext();
            currRow++;
            return true;
        } else {
            if (!finished) {
                currRow++;
            }
            finished = true;
            return false;
        }
    }

    @Override
    public final boolean relative(int rows) throws SQLException {
        checkClosed();
        check(rows >= 0, "data.world JDBC result sets are forward only, cannot move backwards");
        if (rows == 0) {
            // Already at the desired row
            return true;
        }
        // Before the desired row
        int moved = 0;
        while (hasNext() && moved < rows) {
            currItem = moveNext();
            currRow++;
            moved++;
        }
        // If we didn't reach it we hit the end of the result set
        if (moved < rows) {
            finished = true;
            currItem = null;
        }
        return (rows == moved);
    }

    @Override
    public final void setFetchDirection(int direction) throws SQLException {
        checkSupported(direction == ResultSet.FETCH_FORWARD, "data.world JDBC Result Sets only support forward fetch");
    }

    @Override
    public final void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
