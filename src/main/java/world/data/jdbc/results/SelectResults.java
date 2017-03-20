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

/**
 * Represents SPARQL SELECT results
 */
public class SelectResults extends StreamedResults<Binding> {

    private ResultSetPeekable innerResults;
    private List<String> columns;
    private SelectResultsMetadata metadata;

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
        this.columns = new ArrayList<>(this.innerResults.getResultVars());
        this.metadata = new SelectResultsMetadata(this, this.innerResults);
    }

    @Override
    public void closeStreamInternal() {
        if (this.innerResults != null) {
            if (this.innerResults instanceof Closeable) {
                ((Closeable) this.innerResults).close();
            }
            this.innerResults = null;
        }
    }

    public int findColumn(String columnLabel) throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Result Set is closed");
        }
        for (int i = 0; i < this.columns.size(); i++) {
            if (this.columns.get(i).equals(columnLabel)) {
                // Remember that JDBC uses a 1 based index
                return i + 1;
            }
        }
        throw new SQLException("The given column does not exist in this result set");
    }

    @Override
    protected String findColumnLabel(int columnIndex) throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Result Set is closed");
        }
        if (columnIndex >= 1 && columnIndex <= this.columns.size()) {
            // Remember that JDBC uses a 1 based index
            return this.columns.get(columnIndex - 1);
        } else {
            throw new SQLException("Column Index is out of bounds");
        }
    }

    @Override
    protected Node getNode(String columnLabel) throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Result Set is closed");
        }
        if (this.getCurrentRow() == null) {
            throw new SQLException("Not currently at a row");
        }
        if (!this.columns.contains(columnLabel)) {
            throw new SQLException("The given column does not exist in the result set");
        }
        return this.getCurrentRow().get(Var.alloc(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return this.metadata;
    }

    /**
     * Gets whether there are further rows in the underlying SELECT results
     */
    @Override
    protected boolean hasNext() throws SQLException {
        // No null check here because superclass will not call us after we are
        // closed and set to null
        try {
            return this.innerResults.hasNext();
        } catch (QueryCancelledException e) {
            throw new SQLException("Query was cancelled, it is likely that your query exceeded the specified execution timeout",
                    e);
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
            return this.innerResults.nextBinding();
        } catch (QueryCancelledException e) {
            throw new SQLException("Query was cancelled, it is likely that your query exceeded the specified execution timeout",
                    e);
        } catch (Throwable e) {
            // Wrap as SQL exception
            throw new SQLException("Unexpected error while moving through results", e);
        }
    }
}
