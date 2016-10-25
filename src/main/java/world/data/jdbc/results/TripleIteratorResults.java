package world.data.jdbc.results;

import org.apache.jena.atlas.iterator.PeekIterator;
import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryCancelledException;
import org.apache.jena.query.QueryExecution;
import world.data.jdbc.metadata.TripleResultsMetadata;
import world.data.jdbc.statements.DataWorldStatement;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * Represents results of a CONSTRUCT/DESCRIBE query where the results are
 * streamed
 *
 */
public class TripleIteratorResults extends StreamedResults<Triple> {

    private TripleResultsMetadata metadata;
    private PeekIterator<Triple> triples;
    private String subjColumn, predColumn, objColumn;
    private int numColumns;

    /**
     * Creates a new result set which is backed by a triple iterator
     *
     * @param statement
     *            Statement
     * @param qe
     *            Query Execution
     * @param ts
     *            Triple Iterator
     * @throws SQLException
     *             Thrown if there is a problem creating the results
     */
    public TripleIteratorResults(DataWorldStatement statement, QueryExecution qe, Iterator<Triple> ts)
            throws SQLException {
        super(statement, qe);
        this.triples = PeekIterator.create(ts);
        this.metadata = new TripleResultsMetadata(this, this.triples);
        this.numColumns = this.metadata.getColumnCount();
        this.subjColumn = this.metadata.getSubjectColumnLabel();
        this.predColumn = this.metadata.getPredicateColumnLabel();
        this.objColumn = this.metadata.getObjectColumnLabel();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (this.subjColumn != null && this.subjColumn.equals(columnLabel)) {
            return TripleResultsMetadata.COLUMN_INDEX_SUBJECT;
        } else if (this.predColumn != null && this.predColumn.equals(columnLabel)) {
            return this.subjColumn == null ? TripleResultsMetadata.COLUMN_INDEX_SUBJECT
                    : TripleResultsMetadata.COLUMN_INDEX_PREDICATE;
        } else if (this.objColumn != null && this.objColumn.equals(columnLabel)) {
            return this.subjColumn == null && this.predColumn == null ? TripleResultsMetadata.COLUMN_INDEX_SUBJECT
                    : (this.subjColumn == null || this.predColumn == null ? TripleResultsMetadata.COLUMN_INDEX_PREDICATE
                    : TripleResultsMetadata.COLUMN_INDEX_OBJECT);
        } else {
            throw new SQLException("Column " + columnLabel + " does not exist in these results");
        }
    }

    @Override
    protected boolean hasNext() throws SQLException {
        // No null check here because superclass will not call us after we are
        // closed and set to null
        try {
            return this.triples.hasNext();
        } catch (QueryCancelledException e) {
            throw new SQLException("Query was cancelled, it is likely that your query exceeded the specified execution timeout",
                    e);
        } catch (Throwable e) {
            // Wrap as SQL exception
            throw new SQLException("Unexpected error while moving through results", e);
        }
    }

    @Override
    protected Triple moveNext() throws SQLException {
        try {
            return this.triples.next();
        } catch (QueryCancelledException e) {
            throw new SQLException("Query was cancelled, it is likely that your query exceeded the specified execution timeout",
                    e);
        } catch (Throwable e) {
            // Wrap as SQL exception
            throw new SQLException("Unexpected error while moving through results", e);
        }
    }

    @Override
    protected void closeStreamInternal() {
        if (this.triples != null) {
            if (this.triples instanceof Closeable) {
                ((Closeable) this.triples).close();
            }
            this.triples = null;
        }
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return metadata;
    }

    @Override
    protected String findColumnLabel(int columnIndex) throws SQLException {
        if (this.isClosed())
            throw new SQLException("Result Set is closed");
        if (columnIndex >= 1 && columnIndex <= this.numColumns) {
            switch (columnIndex) {
                case TripleResultsMetadata.COLUMN_INDEX_SUBJECT:
                    return this.subjColumn != null ? this.subjColumn : (this.predColumn != null ? this.predColumn : this.objColumn);
                case TripleResultsMetadata.COLUMN_INDEX_PREDICATE:
                    return this.subjColumn != null && this.predColumn != null ? this.predColumn : this.objColumn;
                case TripleResultsMetadata.COLUMN_INDEX_OBJECT:
                    return this.objColumn;
                default:
                    throw new SQLException("Column Index is out of bounds");
            }
        } else {
            throw new SQLException("Column Index is out of bounds");
        }
    }

    @Override
    protected Node getNode(String columnLabel) throws SQLException {
        if (this.isClosed())
            throw new SQLException("Result Set is closed");
        if (this.getCurrentRow() == null)
            throw new SQLException("Not currently at a row");
        Triple t = this.getCurrentRow();
        if (this.subjColumn != null && this.subjColumn.equals(columnLabel)) {
            return t.getSubject();
        } else if (this.predColumn != null && this.predColumn.equals(columnLabel)) {
            return t.getPredicate();
        } else if (this.objColumn != null && this.objColumn.equals(columnLabel)) {
            return t.getObject();
        } else {
            throw new SQLException("Unknown column label");
        }
    }
}
