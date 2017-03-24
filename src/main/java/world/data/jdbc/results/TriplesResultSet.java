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

import org.apache.jena.atlas.iterator.PeekIterator;
import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryCancelledException;
import org.apache.jena.query.QueryExecution;
import world.data.jdbc.metadata.TriplesResultSetMetadata;
import world.data.jdbc.statements.Statement;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;

import static world.data.jdbc.util.Conditions.check;

/**
 * Represents results of a CONSTRUCT/DESCRIBE query where the results are
 * streamed
 */
public class TriplesResultSet extends AbstractStreamingResultSet<Triple> {

    private final TriplesResultSetMetadata metadata;
    private PeekIterator<Triple> triples;
    private final String subjColumn, predColumn, objColumn;
    private final int numColumns;

    /**
     * Creates a new result set which is backed by a triple iterator
     *
     * @param statement Statement
     * @param qe        Query Execution
     * @param ts        Triple Iterator
     * @throws SQLException Thrown if there is a problem creating the results
     */
    public TriplesResultSet(Statement statement, QueryExecution qe, Iterator<Triple> ts)
            throws SQLException {
        super(statement, qe);
        this.triples = PeekIterator.create(ts);
        this.metadata = new TriplesResultSetMetadata(this, triples);
        this.numColumns = metadata.getColumnCount();
        this.subjColumn = metadata.getSubjectColumnLabel();
        this.predColumn = metadata.getPredicateColumnLabel();
        this.objColumn = metadata.getObjectColumnLabel();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (subjColumn != null && subjColumn.equals(columnLabel)) {
            return TriplesResultSetMetadata.COLUMN_INDEX_SUBJECT;
        } else if (predColumn != null && predColumn.equals(columnLabel)) {
            return subjColumn == null ? TriplesResultSetMetadata.COLUMN_INDEX_SUBJECT
                    : TriplesResultSetMetadata.COLUMN_INDEX_PREDICATE;
        } else if (objColumn != null && objColumn.equals(columnLabel)) {
            return subjColumn == null && predColumn == null ? TriplesResultSetMetadata.COLUMN_INDEX_SUBJECT
                    : (subjColumn == null || predColumn == null ? TriplesResultSetMetadata.COLUMN_INDEX_PREDICATE
                    : TriplesResultSetMetadata.COLUMN_INDEX_OBJECT);
        } else {
            throw new SQLException("Column " + columnLabel + " does not exist in these results");
        }
    }

    @Override
    protected boolean hasNext() throws SQLException {
        // No null check here because superclass will not call us after we are
        // closed and set to null
        try {
            return triples.hasNext();
        } catch (QueryCancelledException e) {
            throw new SQLException("Query was cancelled, it is likely that your query exceeded the specified execution timeout", e);
        } catch (Throwable e) {
            // Wrap as SQL exception
            throw new SQLException("Unexpected error while moving through results", e);
        }
    }

    @Override
    protected Triple moveNext() throws SQLException {
        try {
            return triples.next();
        } catch (QueryCancelledException e) {
            throw new SQLException("Query was cancelled, it is likely that your query exceeded the specified execution timeout", e);
        } catch (Throwable e) {
            // Wrap as SQL exception
            throw new SQLException("Unexpected error while moving through results", e);
        }
    }

    @Override
    protected void closeStreamInternal() {
        if (triples != null) {
            if (triples instanceof Closeable) {
                ((Closeable) triples).close();
            }
            triples = null;
        }
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return metadata;
    }

    @Override
    protected String findColumnLabel(int columnIndex) throws SQLException {
        checkClosed();
        check(columnIndex >= 1 && columnIndex <= numColumns, "Column Index is out of bounds");
        switch (columnIndex) {
            case TriplesResultSetMetadata.COLUMN_INDEX_SUBJECT:
                return subjColumn != null ? subjColumn : (predColumn != null ? predColumn : objColumn);
            case TriplesResultSetMetadata.COLUMN_INDEX_PREDICATE:
                return subjColumn != null && predColumn != null ? predColumn : objColumn;
            case TriplesResultSetMetadata.COLUMN_INDEX_OBJECT:
                return objColumn;
            default:
                throw new SQLException("Column Index is out of bounds");
        }
    }

    @Override
    protected Node getNode(String columnLabel) throws SQLException {
        checkClosed();
        check(getCurrentRow() != null, "Not currently at a row");
        Triple t = getCurrentRow();
        if (subjColumn != null && subjColumn.equals(columnLabel)) {
            return t.getSubject();
        } else if (predColumn != null && predColumn.equals(columnLabel)) {
            return t.getPredicate();
        } else if (objColumn != null && objColumn.equals(columnLabel)) {
            return t.getObject();
        } else {
            throw new SQLException("Unknown column label");
        }
    }
}
