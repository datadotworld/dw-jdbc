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
package world.data.jdbc.statements;

import org.apache.jena.jdbc.JdbcCompatibility;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
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

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.util.Conditions.check;
import static world.data.jdbc.util.Conditions.checkSupported;

public class DataWorldStatement implements Statement {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataWorldStatement.class);

    private static final int NO_LIMIT = 0;
    private static final int USE_CONNECTION_COMPATIBILITY = Integer.MIN_VALUE;

    private int timeout = NO_LIMIT;
    private int compatibilityLevel = USE_CONNECTION_COMPATIBILITY;
    private SQLWarning warnings = null;

    private final QueryBuilder queryBuilder;
    private final DataWorldConnection connection;

    private final List<String> commands = new ArrayList<>();
    private final Queue<ResultSet> results = new LinkedList<>();
    private final List<ResultSet> openResults = new ArrayList<>();
    private ResultSet currResults;
    private boolean closed;

    public DataWorldStatement(final DataWorldConnection connection, final QueryBuilder queryBuilder) {
        this.connection = requireNonNull(connection, "connection");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder");
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
        if (compatibilityLevel == USE_CONNECTION_COMPATIBILITY) {
            return connection.getJdbcCompatibilityLevel();
        }
        return compatibilityLevel;
    }

    /**
     * Sets the JDBC compatibility level that is in use, see
     * {@link JdbcCompatibility} for explanations.
     * <p>
     * By default this is set at the connection level and inherited, however you
     * may call the {@code setJdbcCompatibilityLevel} method to set the compatibility
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
            compatibilityLevel = USE_CONNECTION_COMPATIBILITY;
        } else {
            compatibilityLevel = JdbcCompatibility.normalizeLevel(level);
        }
    }

    public void clearWarnings() {
        warnings = null;
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
        return NO_LIMIT;
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
        return -1;
    }

    public SQLWarning getWarnings() {
        return warnings;
    }

    /**
     * Helper method that derived classes may use to set warnings
     *
     * @param warning Warning
     */
    private void setWarning(SQLWarning warning) {
        LOGGER.warn("SQL Warning was issued", warning);
        if (warnings == null) {
            warnings = warning;
        } else {
            // Chain with existing warnings
            warning.setNextWarning(warnings);
            warnings = warning;
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
        commands.add(sql);
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    protected QueryEngineHTTP createQueryExecution(Query q) throws SQLException {
        return connection.createQueryExecution(q);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public final int[] executeBatch() throws SQLException {
        checkClosed();

        // Go ahead and process the batch
        int[] rets = new int[commands.size()];
        ResultSet curr = currResults;
        for (int i = 0; i < commands.size(); i++) {
            if (execute(commands.get(i))) {
                // True means it returned a ResultSet
                results.add(getResultSet());
                currResults = null;
                rets[i] = SUCCESS_NO_INFO;
            } else {
                // Need to add a null to getMoreResults() to produce correct
                // behavior across subsequent calls to getMoreResults()
                results.add(null);
                rets[i] = -1;
            }
        }
        currResults = curr;
        // Make the next available results the current results if there
        // are no current results
        if (currResults == null && !results.isEmpty()) {
            currResults = results.poll();
        }
        return rets;
    }

    @Override
    public void clearBatch() {
        commands.clear();
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        LOGGER.info("Closing statement");
        closed = true;
        // Close current result set (if any)
        if (currResults != null) {
            currResults.close();
            currResults = null;
        }
        // Close any remaining open results
        if (results.size() > 0 || openResults.size() > 0) {
            LOGGER.info("Closing " + (results.size() + openResults.size()) + " open result sets");

            // Queue results i.e. stuff resulting from a query that produced
            // multiple result sets or executeBatch() calls
            while (!results.isEmpty()) {
                ResultSet rset = results.poll();
                if (rset != null) {
                    rset.close();
                }
            }
            // Close open result sets i.e. stuff left around depending on
            // statement correction
            for (ResultSet rset : openResults) {
                rset.close();
            }
            openResults.clear();
            LOGGER.info("All open result sets were closed");
        }
        LOGGER.info("Statement was closed");
    }

    private void setWarning(String warning) {
        setWarning(new SQLWarning(warning));
    }

    public void setEscapeProcessing(boolean enable) {
    }

    public void setFetchDirection(int direction) throws SQLException {
        checkSupported(direction == ResultSet.FETCH_FORWARD, "Only ResultSet.FETCH_FORWARD is supported as a fetch direction");
    }

    public void setFetchSize(int rows) {
        setWarning("setMaxFieldSize() was called but there is no fetch size control for data.world JDBC connections");
    }

    public void setMaxFieldSize(int max) {
        // Ignored
        setWarning("setMaxFieldSize() was called but there is no field size limit for data.world JDBC connections");
    }

    public void setMaxRows(int max) {
        setWarning("setMaxRows() was called but there is no row size limit for data.world JDBC connections");
    }

    public void setPoolable(boolean poolable) {
        setWarning("setPoolable() was called but data.world JDBC statements are always considered poolable");
    }

    public void setQueryTimeout(int seconds) {
        timeout = Math.max(seconds, 0);
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

    public final boolean isPoolable() {
        return true;
    }

    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final boolean execute(String sql) throws SQLException {
        checkClosed();

        // Pre-process the command text
        LOGGER.info("Received input command text:\n {}", sql);

        Query q = queryBuilder.buildQuery(sql);
        check(q != null, "Unable to create a query/update");

        // Execute as a query
        LOGGER.info("Treating command text as a query");
        return executeQuery(q);
    }

    @Override
    public final ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();

        // Pre-process the command text
        LOGGER.info("Received input command text:\n {}", sql);

        Query q = queryBuilder.buildQuery(sql);
        check(q != null, "Unable to create a query");

        boolean hasResultSet = executeQuery(q);
        check(hasResultSet, "Query did not produce a result set");
        return currResults;
    }

    private boolean executeQuery(Query q) throws SQLException {
        try {
            // Create the query execution
            QueryExecution qe = createQueryExecution(q);

            // Manipulate the query execution if appropriate
            if (timeout > NO_LIMIT) {
                qe.setTimeout(timeout, TimeUnit.SECONDS, timeout, TimeUnit.SECONDS);
            }

            currResults = queryBuilder.buildResults(this, q, qe);
            return true;
        } catch (SQLException e) {
            throw e;
        } catch (Throwable e) {
            LOGGER.error("Query evaluation failed", e);
            throw new SQLException("Error occurred during query evaluation", e);
        }
    }

    @Override
    public final int executeUpdate(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final Connection getConnection() {
        return connection;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();

        if (currResults != null) {
            currResults.close();
            currResults = null;
        }
        if (!results.isEmpty()) {
            currResults = results.poll();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();

        switch (current) {
            case Statement.CLOSE_CURRENT_RESULT:
                return getMoreResults();
            case Statement.CLOSE_ALL_RESULTS:
                for (ResultSet rset : openResults) {
                    rset.close();
                }
                openResults.clear();
                return getMoreResults();
            case Statement.KEEP_CURRENT_RESULT:
                if (currResults != null) {
                    openResults.add(currResults);
                    currResults = null;
                }
                return getMoreResults();
            default:
                throw new SQLFeatureNotSupportedException(
                        "Unsupported mode for dealing with current results, only Statement.CLOSE_CURRENT_RESULT, Statement.CLOSE_ALL_RESULTS and Statement.KEEP_CURRENT_RESULT are supported");
        }
    }

    @Override
    public int getQueryTimeout() {
        return timeout;
    }

    @Override
    public final ResultSet getResultSet() throws SQLException {
        checkClosed();
        return currResults;
    }

    @Override
    public final boolean isClosed() {
        return closed;
    }

    private void checkClosed() throws SQLException {
        check(!closed, "Statement is closed");
    }
}
