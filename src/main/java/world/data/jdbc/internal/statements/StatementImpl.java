/*
* dw-jdbc
* Copyright 2017 data.world, Inc.

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
package world.data.jdbc.internal.statements;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import world.data.jdbc.DataWorldConnection;
import world.data.jdbc.DataWorldStatement;
import world.data.jdbc.JdbcCompatibility;
import world.data.jdbc.internal.query.QueryEngine;
import world.data.jdbc.internal.util.ResourceContainer;
import world.data.jdbc.internal.util.ResourceManager;
import world.data.jdbc.model.Node;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.internal.util.Conditions.check;
import static world.data.jdbc.internal.util.Conditions.checkSupported;
import static world.data.jdbc.internal.util.Optionals.or;

@Log
public class StatementImpl implements DataWorldStatement, ReadOnlyStatement, ResourceContainer {

    private static final int NO_LIMIT = 0;

    private int timeout = NO_LIMIT;
    private JdbcCompatibility compatibilityLevel;
    private SQLWarning warnings = null;

    final QueryEngine queryEngine;
    private final DataWorldConnection connection;
    private final ResourceManager resources = new ResourceManager();

    private final List<BatchItem> commands = new ArrayList<>();
    private final Queue<ResultSet> batchResults = new LinkedList<>();
    private final List<ResultSet> openResults = new ArrayList<>();
    private ResultSet currResults;
    private boolean closed;

    public StatementImpl(QueryEngine queryEngine, DataWorldConnection connection) {
        this.queryEngine = requireNonNull(queryEngine, "queryEngine");
        this.connection = requireNonNull(connection, "connection");
        ((ResourceContainer) connection).getResources().register(this);
    }

    @Override
    public ResourceManager getResources() {
        return resources;
    }

    /**
     * Gets the JDBC compatibility level that is in use, see
     * {@link JdbcCompatibility} for explanations
     * <p>
     * By default this is set at the connection level and inherited, however you
     * may call {@link #setJdbcCompatibilityLevel(JdbcCompatibility)} to set the compatibility
     * level for this statement. This allows you to change the compatibility
     * level on a per-query basis if so desired.
     * </p>
     *
     * @return Compatibility level
     */
    @Override
    public final JdbcCompatibility getJdbcCompatibilityLevel() throws SQLException {
        checkClosed();
        return or(compatibilityLevel, connection.getJdbcCompatibilityLevel());
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
     * @param compatibilityLevel Compatibility level
     */
    @Override
    public final void setJdbcCompatibilityLevel(JdbcCompatibility compatibilityLevel) throws SQLException {
        checkClosed();
        this.compatibilityLevel = compatibilityLevel;
    }

    @Override
    public final DataWorldConnection getConnection() {
        return connection;
    }

    @Override
    public final void clearWarnings() {
        warnings = null;
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
    public final int getMaxFieldSize() {
        return NO_LIMIT;
    }

    @Override
    public final int getMaxRows() {
        return NO_LIMIT;
    }

    /**
     * Gets that result sets are read-only
     */
    @Override
    public final int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public final int getResultSetHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public final int getResultSetType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public final SQLWarning getWarnings() {
        return warnings;
    }

    /**
     * Helper method that derived classes may use to set warnings
     *
     * @param warning Warning
     */
    private void setWarning(SQLWarning warning) {
        log.warning("SQL Warning was issued: " + warning);
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
        return DataWorldStatement.class.equals(iface);
    }

    @Override
    public final <T> T unwrap(Class<T> iface) throws SQLException {
        check(isWrapperFor(iface), "Not a wrapper for the desired interface");
        return iface.cast(this);
    }

    @Override
    public final void addBatch(String query) throws SQLException {
        checkClosed();
        doAddBatch(query, Collections.emptyMap());
    }

    void doAddBatch(String query, Map<String, Node> params) throws SQLException {
        commands.add(new BatchItem(query, params));
    }

    @Override
    public final void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final boolean execute(String query, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        checkSupported(autoGeneratedKeys == NO_GENERATED_KEYS, "Does not support auto-generated keys");
        return execute(query);
    }

    @Override
    public final boolean execute(String query, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final boolean execute(String query, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final int[] executeBatch() throws SQLException {
        checkClosed();

        // Go ahead and process the batch
        int[] rets = new int[commands.size()];
        ResultSet curr = currResults;
        for (int i = 0; i < commands.size(); i++) {
            BatchItem batchItem = commands.get(i);
            if (doExecuteQuery(batchItem.query, batchItem.params)) {
                // True means it returned a ResultSet
                batchResults.add(getResultSet());
                currResults = null;
                rets[i] = SUCCESS_NO_INFO;
            } else {
                // Need to add a null to getMoreResults() to produce correct
                // behavior across subsequent calls to getMoreResults()
                batchResults.add(null);
                rets[i] = -1;
            }
        }
        currResults = curr;
        // Make the next available results the current results if there
        // are no current results
        if (currResults == null && !batchResults.isEmpty()) {
            currResults = batchResults.poll();
        }
        return rets;
    }

    @Override
    public final void clearBatch() {
        commands.clear();
    }

    @Override
    public final void close() throws SQLException {
        if (closed) {
            return;
        }
        log.fine("Closing statement");
        ((ResourceContainer) connection).getResources().remove(this);
        try {
            // Close results that are still open
            resources.close();
        } catch (Exception e) {
            log.warning("Unexpected trying to close resources: " + e);
        } finally {
            closed = true;
            log.fine("Statement was closed");
        }
    }

    private void setWarning(String warning) {
        setWarning(new SQLWarning(warning));
    }

    @Override
    public final void setEscapeProcessing(boolean enable) {
        // Ignored, no-op
    }

    @Override
    public final void setFetchDirection(int direction) throws SQLException {
        checkSupported(direction == ResultSet.FETCH_FORWARD, "Only ResultSet.FETCH_FORWARD is supported as a fetch direction");
    }

    @Override
    public final void setFetchSize(int rows) {
        setWarning("setMaxFieldSize() was called but there is no fetch size control for data.world JDBC connections");
    }

    @Override
    public final void setMaxFieldSize(int max) {
        // Ignored
        setWarning("setMaxFieldSize() was called but there is no field size limit for data.world JDBC connections");
    }

    @Override
    public final void setMaxRows(int max) {
        setWarning("setMaxRows() was called but there is no row size limit for data.world JDBC connections");
    }

    @Override
    public final void setPoolable(boolean poolable) {
        setWarning("setPoolable() was called but data.world JDBC statements are always considered poolable");
    }

    @Override
    public final void setQueryTimeout(int seconds) {
        timeout = Math.max(seconds, 0);
    }

    // Java 6/7 compatibility
    @Override
    @SuppressWarnings("javadoc")
    public final boolean isCloseOnCompletion() {
        // Statements do not automatically close
        return false;
    }

    @Override
    @SuppressWarnings("javadoc")
    public final void closeOnCompletion() throws SQLException {
        // We don't support the JDBC 4.1 feature of closing statements
        // automatically
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final boolean isPoolable() {
        return true;
    }

    @Override
    public final boolean execute(String query) throws SQLException {
        checkClosed();
        return doExecuteQuery(query, Collections.emptyMap());
    }

    @Override
    public final ResultSet executeQuery(String query) throws SQLException {
        checkClosed();
        boolean hasResultSet = doExecuteQuery(query, Collections.emptyMap());
        check(hasResultSet, "Query did not produce a result set");
        return getResultSet();
    }

    boolean doExecuteQuery(String query, Map<String, Node> parameters) throws SQLException {
        log.fine(() -> "Received input command text:\n " + query);
        try {
            currResults = queryEngine.execute(this, query, parameters, timeout != 0 ? timeout : null);
            return true;
        } catch (SQLException e) {
            throw e;
        } catch (Throwable e) {
            throw new SQLException("Error occurred during query evaluation", e);
        }
    }

    @Override
    public final boolean getMoreResults() throws SQLException {
        checkClosed();
        if (currResults != null) {
            currResults.close();
            currResults = null;
        }
        if (!batchResults.isEmpty()) {
            currResults = batchResults.poll();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public final boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        switch (current) {
            case CLOSE_CURRENT_RESULT:
                return getMoreResults();
            case CLOSE_ALL_RESULTS:
                for (ResultSet rset : openResults) {
                    rset.close();
                }
                openResults.clear();
                return getMoreResults();
            case KEEP_CURRENT_RESULT:
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
    public final int getQueryTimeout() {
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

    void checkClosed() throws SQLException {
        check(!closed, "Statement is closed");
    }

    @RequiredArgsConstructor
    private static class BatchItem {
        private final String query;
        private final Map<String, Node> params;
    }
}
