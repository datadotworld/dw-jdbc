package world.data.jdbc.statements;

import org.apache.jena.atlas.web.auth.HttpAuthenticator;
import org.apache.jena.jdbc.JdbcCompatibility;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import world.data.jdbc.connections.DataWorldConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class DataWorldStatement implements Statement{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataWorldStatement.class);
    private static final int NO_LIMIT = 0;
    private static final int USE_CONNECTION_COMPATIBILITY = Integer.MIN_VALUE;
    private int maxRows = NO_LIMIT;
    @SuppressWarnings("unused")
    private int timeout = NO_LIMIT;
    private int compatibilityLevel = USE_CONNECTION_COMPATIBILITY;
    private SQLWarning warnings = null;
    private int updateCount = 0;

    private final HttpAuthenticator authenticator;
    private final QueryBuilder queryBuilder;
    private final DataWorldConnection connection;

    private final List<String> commands = new ArrayList<>();
    private final Queue<ResultSet> results = new LinkedList<>();
    private final List<ResultSet> openResults = new ArrayList<>();
    private ResultSet currResults = null;
    private boolean closed = false;

    public DataWorldStatement(final DataWorldConnection connection, final HttpAuthenticator authenticator, final QueryBuilder queryBuilder) {
        this.authenticator = authenticator;
        this.connection = connection;
        this.queryBuilder = queryBuilder;
    }

    /**
     * Gets the JDBC compatibility level that is in use, see
     * {@link JdbcCompatibility} for explanations
     * <p>
     * By default this is set at the connection level and inherited, however you
     * may call {@link #setJdbcCompatibilityLevel(int)} to set the compatibility
     * level for this statement. This allows you to change the compatibility
     * level on a per-query basis if so desired.
     * </p>
     *
     * @return Compatibility level
     */
    public int getJdbcCompatibilityLevel() {
        if (this.compatibilityLevel == USE_CONNECTION_COMPATIBILITY)
            return this.connection.getJdbcCompatibilityLevel();
        return this.compatibilityLevel;
    }

    /**
     * Sets the JDBC compatibility level that is in use, see
     * {@link JdbcCompatibility} for explanations.
     * <p>
     * By default this is set at the connection level and inherited, however you
     * may call {@link #setJdbcCompatibilityLevel(int)} to set the compatibility
     * level for this statement. This allows you to change the compatibility
     * level on a per-query basis if so desired.
     * </p>
     * <p>
     * Changing the level may not effect existing open objects, behaviour in
     * this case will be implementation specific.
     * </p>
     *
     * @param level Compatibility level
     */
    public void setJdbcCompatibilityLevel(int level) {
        if (level == USE_CONNECTION_COMPATIBILITY) {
            this.compatibilityLevel = USE_CONNECTION_COMPATIBILITY;
        } else {
            this.compatibilityLevel = JdbcCompatibility.normalizeLevel(level);
        }
    }

    public void clearWarnings() {
        this.warnings = null;
    }

    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    public int getFetchSize() {
        return 0;
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxFieldSize() {
        return NO_LIMIT;
    }

    public int getMaxRows() {
        return maxRows;
    }

    /**
     * Gets that result sets are read-only
     */
    public final int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    public int getResultSetHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    public final int getResultSetType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public int getUpdateCount() {
        return this.updateCount;
    }

    public SQLWarning getWarnings() {
        return this.warnings;
    }

    /**
     * Helper method that derived classes may use to set warnings
     *
     * @param warning Warning
     */
    private void setWarning(SQLWarning warning) {
        LOGGER.warn("SQL Warning was issued", warning);
        if (this.warnings == null) {
            this.warnings = warning;
        } else {
            // Chain with existing warnings
            warning.setNextWarning(this.warnings);
            this.warnings = warning;
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void addBatch(String sql) {
        this.commands.add(sql);
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    protected QueryEngineHTTP createQueryExecution(Query q) throws SQLException {
        if (this.connection.getQueryEndpoint() == null)
            throw new SQLException("This statement is backed by a write-only connection, read operations are not supported");

        // Create basic execution
        QueryEngineHTTP exec = (QueryEngineHTTP) QueryExecutionFactory.sparqlService(this.connection.getQueryEndpoint(), q);

        // Apply authentication settings
        if (this.authenticator != null) {
            exec.setAuthenticator(authenticator);
        }
        // Return execution
        return exec;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return this.execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return this.execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return this.execute(sql);
    }

    @Override
    public final int[] executeBatch() throws SQLException {
        if (this.isClosed())
            throw new SQLException("The Statement is closed");

        // Go ahead and process the batch
        int[] rets = new int[this.commands.size()];
        ResultSet curr = this.currResults;
        for (int i = 0; i < this.commands.size(); i++) {
            if (this.execute(this.commands.get(i))) {
                // True means it returned a ResultSet
                this.results.add(this.getResultSet());
                this.currResults = null;
                rets[i] = SUCCESS_NO_INFO;
            } else {
                // Need to add a null to getMoreResults() to produce correct
                // behavior across subsequent calls to getMoreResults()
                this.results.add(null);
                rets[i] = this.getUpdateCount();
            }
        }
        this.currResults = curr;
        // Make the next available results the current results if there
        // are no current results
        if (this.currResults == null && !this.results.isEmpty()) {
            this.currResults = this.results.poll();
        }
        return rets;
    }

    @Override
    public void clearBatch() {
        this.commands.clear();
    }

    @Override
    public void close() throws SQLException {
        if (this.closed)
            return;
        LOGGER.info("Closing statement");
        this.closed = true;
        // Close current result set (if any)
        if (this.currResults != null) {
            this.currResults.close();
            this.currResults = null;
        }
        // Close any remaining open results
        if (this.results.size() > 0 || this.openResults.size() > 0) {
            LOGGER.info("Closing " + (this.results.size() + this.openResults.size()) + " open result sets");

            // Queue results i.e. stuff resulting from a query that produced
            // multiple result sets or executeBatch() calls
            while (!this.results.isEmpty()) {
                ResultSet rset = this.results.poll();
                if (rset != null)
                    rset.close();
            }
            // Close open result sets i.e. stuff left around depending on
            // statement correction
            for (ResultSet rset : this.openResults) {
                rset.close();
            }
            this.openResults.clear();
            LOGGER.info("All open result sets were closed");
        }
        LOGGER.info("Statement was closed");
    }
    /**
     * Helper method that derived classes may use to set warnings
     *
     * @param warning Warning
     */
    protected void setWarning(String warning) {
        this.setWarning(new SQLWarning(warning));
    }

    /**
     * Helper method that derived classes may use to set warnings
     *
     * @param warning Warning
     * @param cause   Cause
     */
    protected void setWarning(String warning, Throwable cause) {
        this.setWarning(new SQLWarning(warning, cause));
    }

    public void setEscapeProcessing(boolean enable) {
    }

    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD)
            throw new SQLFeatureNotSupportedException("Only ResultSet.FETCH_FORWARD is supported as a fetch direction");

    }

    public void setFetchSize(int rows) {
        this.setWarning("setMaxFieldSize() was called but there is no fetch size control for data.world JDBC connections");
    }

    public void setMaxFieldSize(int max) {
        // Ignored
        this.setWarning("setMaxFieldSize() was called but there is no field size limit for data.world JDBC connections");
    }

    public void setMaxRows(int max) {
        if (max <= NO_LIMIT) {
            this.maxRows = NO_LIMIT;
        } else {
            this.maxRows = max;
        }
    }

    public void setPoolable(boolean poolable) {
        // Ignored
        this.setWarning("setPoolable() was called but data.world JDBC statements are always considered poolable");
    }

    public void setQueryTimeout(int seconds) {
        if (seconds <= NO_LIMIT) {
            this.timeout = NO_LIMIT;
        } else {
            this.timeout = seconds;
        }
    }

    // Java 6/7 compatibility
    @SuppressWarnings("javadoc")
    public boolean isCloseOnCompletion() {
        // Statements do not automatically close
        return false;
    }

    @SuppressWarnings("javadoc")
    public void closeOnCompletion() throws SQLException {
        // We don't support the JDBC 4.1 feature of closing statements
        // automatically
        throw new SQLFeatureNotSupportedException();
    }

    protected void beginTransaction(ReadWrite type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions against remote endpoint backed connections are not supported");
    }

    protected void commitTransaction() throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions against remote endpoint backed connections are not supported");
    }

    protected void rollbackTransaction() throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions against remote endpoint backed connections are not supported");
    }

    protected boolean hasActiveTransaction() {
        // Remote endpoints don't support transactions so can't ever have an
        // active transaction
        return false;
    }

    public final boolean isPoolable() {
        return true;
    }

    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final boolean execute(String sql) throws SQLException {
        if (this.isClosed())
            throw new SQLException("The Statement is closed");

        // Pre-process the command text
        LOGGER.info("Received input command text:\n {}", sql);

        Query q = queryBuilder.buildQuery(sql);

        if (q != null) {
            // Execute as a query
            LOGGER.info("Treating command text as a query");
            return this.executeQuery(q);
        } else {
            throw new SQLException("Unable to create a SPARQL query/update");
        }
    }

    @Override
    public final ResultSet executeQuery(String sql) throws SQLException {
        if (this.isClosed())
            throw new SQLException("The Statement is closed");

        // Pre-process the command text
        LOGGER.info("Received input command text:\n {}", sql);

        Query q = queryBuilder.buildQuery(sql);

        if (q == null)
            throw new SQLException("Unable to create a SQL Query");
        if (this.executeQuery(q)) {
            return this.currResults;
        } else {
            throw new SQLException("Query did not produce a result set");
        }
    }

    protected boolean executeQuery(Query q) throws SQLException {
        if (this.isClosed())
            throw new SQLException("The Statement is closed");

        try {
            // Manipulate the query if appropriate
            if (this.maxRows > NO_LIMIT) {
                // If we have no LIMIT or the LIMIT is greater than the
                // permitted max rows
                // then we will set the LIMIT to the max rows
                if (!q.hasLimit() || q.getLimit() > this.maxRows) {
                    LOGGER.info("Enforced max rows on results by applying LIMIT {} to the query", this.maxRows);
                    q.setLimit(this.maxRows);
                }
            }

            // Create the query execution
            QueryExecution qe = this.createQueryExecution(q);

            // Manipulate the query execution if appropriate
            if (this.timeout > NO_LIMIT) {
                qe.setTimeout(this.timeout, TimeUnit.SECONDS, this.timeout, TimeUnit.SECONDS);
            }

            this.currResults = queryBuilder.buildResults(this, q, qe);
            return true;
        } catch (SQLException e) {
            throw e;
        } catch (Throwable e) {
            LOGGER.error("SPARQL Query evaluation failed", e);
            throw new SQLException("Error occurred during SPARQL query evaluation", e);
        }
    }

    @Override
    public final int executeUpdate(String sql) throws SQLException {
        return -1;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return this.executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return this.executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return this.executeUpdate(sql);
    }

    @Override
    public final Connection getConnection() {
        return this.connection;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (this.isClosed())
            throw new SQLException("The Statement is closed");

        if (this.currResults != null) {
            this.currResults.close();
            this.currResults = null;
        }
        if (!this.results.isEmpty()) {
            this.currResults = this.results.poll();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (this.isClosed())
            throw new SQLException("The Statement is closed");

        switch (current) {
            case Statement.CLOSE_CURRENT_RESULT:
                return this.getMoreResults();
            case Statement.CLOSE_ALL_RESULTS:
                for (ResultSet rset : this.openResults) {
                    rset.close();
                }
                this.openResults.clear();
                return this.getMoreResults();
            case Statement.KEEP_CURRENT_RESULT:
                if (this.currResults != null) {
                    this.openResults.add(this.currResults);
                    this.currResults = null;
                }
                return this.getMoreResults();
            default:
                throw new SQLFeatureNotSupportedException(
                        "Unsupported mode for dealing with current results, only Statement.CLOSE_CURRENT_RESULT, Statement.CLOSE_ALL_RESULTS and Statement.KEEP_CURRENT_RESULT are supported");
        }
    }

    @Override
    public int getQueryTimeout() {
        return this.timeout;
    }

    @Override
    public final ResultSet getResultSet() throws SQLException {
        if (this.isClosed())
            throw new SQLException("The Statement is closed");
        return this.currResults;
    }

    @Override
    public final boolean isClosed() {
        return this.closed;
    }
}
