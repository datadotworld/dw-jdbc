package world.data.jdbc.results;

import org.apache.jena.query.QueryExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import world.data.jdbc.statements.DataWorldStatement;

import java.sql.SQLException;

/**
 * Abstract base class for result sets that are backed by a {@link QueryExecution}
 *
 */
public abstract class QueryExecutionResults extends DataWorldResultsSet {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutionResults.class);

    private QueryExecution qe;

    /**
     * Creates new Query Execution backed results
     * @param statement Statement
     * @param qe Query Execution
     * @throws SQLException Thrown if there is an issue creating the results
     */
    public QueryExecutionResults(DataWorldStatement statement, QueryExecution qe) throws SQLException {
        super(statement);
        if (qe == null) throw new SQLException("Query Execution cannot be null");
        this.qe = qe;
    }

    /**
     * Closes the results which also closes the underlying {@link QueryExecution}
     */
    @Override
    public final void close() throws SQLException {
        if (this.qe != null) {
            try {
                // Close the query execution
                this.qe.close();
            } catch (Exception e) {
                LOGGER.error("Unexpected error closing underlying query execution", e);
                throw new SQLException("Unexpected error closing the query execution", e);
            } finally {
                this.qe = null;
            }
        }
        this.closeInternal();
    }

    /**
     * Method which derived classes must implement to provide their own close logic
     * @throws SQLException Thrown if there is an issue closing the results
     */
    protected abstract void closeInternal() throws SQLException;

    @Override
    public final boolean isClosed() {
        return this.qe == null;
    }



}
