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


import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.graph.Node;
import org.apache.jena.query.QueryCancelledException;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.resultset.ResultSetPeekable;
import world.data.jdbc.metadata.SelectResultsMetadata;
import world.data.jdbc.statements.DataWorldStatement;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static world.data.jdbc.util.Conditions.check;

/**
 * Represents SPARQL SELECT results
 */
public class SelectResults extends AbstractStreamedResults<Binding> {

    private ResultSetPeekable innerResults;
    private final List<String> columns;
    private final SelectResultsMetadata metadata;

    /**
     * Creates new select results
     *
     * @param statement Statement that created the result set
     * @param qe        Query Execution
     * @param results   SPARQL Results
     * @throws SQLException Thrown if the arguments are invalid
     */
    public SelectResults(DataWorldStatement statement, QueryExecution qe, org.apache.jena.query.ResultSet results)
            throws SQLException {
        super(statement, qe);
        this.innerResults = ResultSetFactory.makePeekable(results);
        this.columns = new ArrayList<>(innerResults.getResultVars());
        this.metadata = new SelectResultsMetadata(this, innerResults);
    }

    @Override
    public void closeStreamInternal() {
        if (innerResults != null) {
            if (innerResults instanceof Closeable) {
                ((Closeable) innerResults).close();
            }
            innerResults = null;
        }
    }

    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(columnLabel)) {
                // Remember that JDBC uses a 1 based index
                return i + 1;
            }
        }
        throw new SQLException("The given column does not exist in this result set");
    }

    @Override
    protected String findColumnLabel(int columnIndex) throws SQLException {
        checkClosed();
        check(columnIndex >= 1 && columnIndex <= columns.size(), "Column Index is out of bounds");
        // Remember that JDBC uses a 1 based index
        return columns.get(columnIndex - 1);
    }

    @Override
    protected Node getNode(String columnLabel) throws SQLException {
        checkClosed();
        check(getCurrentRow() != null, "Not currently at a row");
        check(columns.contains(columnLabel), "The given column does not exist in the result set");
        return getCurrentRow().get(Var.alloc(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return metadata;
    }

    /**
     * Gets whether there are further rows in the underlying SELECT results
     */
    @Override
    protected boolean hasNext() throws SQLException {
        // No null check here because superclass will not call us after we are
        // closed and set to null
        try {
            return innerResults.hasNext();
        } catch (QueryCancelledException e) {
            throw new SQLException("Query was cancelled, it is likely that your query exceeded the specified execution timeout", e);
        } catch (Throwable e) {
            // Wrap as SQL exception
            throw new SQLException("Unexpected error while moving through results", e);
        }
    }

    /**
     * Gets the next row from the underlying SELECT results
     */
    @Override
    protected Binding moveNext() throws SQLException {
        // No null check here because superclass will not call us after we are
        // closed and set to null
        try {
            return innerResults.nextBinding();
        } catch (QueryCancelledException e) {
            throw new SQLException("Query was cancelled, it is likely that your query exceeded the specified execution timeout", e);
        } catch (Throwable e) {
            // Wrap as SQL exception
            throw new SQLException("Unexpected error while moving through results", e);
        }
    }
}
