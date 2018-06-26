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
package world.data.jdbc.internal.connections;

import lombok.extern.java.Log;
import world.data.jdbc.DataWorldCallableStatement;
import world.data.jdbc.DataWorldConnection;
import world.data.jdbc.DataWorldPreparedStatement;
import world.data.jdbc.DataWorldStatement;
import world.data.jdbc.JdbcCompatibility;
import world.data.jdbc.internal.query.QueryEngine;
import world.data.jdbc.internal.statements.CallableStatementImpl;
import world.data.jdbc.internal.statements.PreparedStatementImpl;
import world.data.jdbc.internal.statements.StatementImpl;
import world.data.jdbc.internal.util.ResourceContainer;
import world.data.jdbc.internal.util.ResourceManager;
import world.data.jdbc.internal.util.WarningList;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.internal.util.Conditions.check;
import static world.data.jdbc.internal.util.Conditions.checkConnectionTransactionIsolation;
import static world.data.jdbc.internal.util.Conditions.checkResultSetHoldability;
import static world.data.jdbc.internal.util.Conditions.checkStatementGeneratedKeys;
import static world.data.jdbc.internal.util.Conditions.checkSupported;
import static world.data.jdbc.internal.util.Optionals.or;

/**
 * Abstract base implementation of a JDBC connection.
 * <p>
 * Generally speaking this is a faithful implementation of a JDBC connection but
 * it also provides a couple of JDBC specific features:
 * </p>
 * <ol>
 * <li>JDBC compatibility level</li>
 * <li>Command pre-processors</li>
 * </ol>
 * <p>
 * The JDBC compatibility level allows the API to behave slightly differently
 * depending on how JDBC like you need it to be, see {@link JdbcCompatibility}
 * for more discussion on this.
 * </p>
 * <p>
 * Command pre-processors are an extension mechanism designed to allow JDBC
 * connections to cope with the fact that the tools consuming the API may be
 * completely unaware that we speak SPARQL rather than SQL. They allow for
 * manipulation of incoming command text as well as manipulation of the parsed
 * SPARQL queries and updates as desired.
 * </p>
 */
@Log
public final class ConnectionImpl implements DataWorldConnection, ResourceContainer {
    private final QueryEngine queryEngine;
    private final DatabaseMetaData metadata;
    private final ResourceManager resources = new ResourceManager();
    private final WarningList warnings = new WarningList();

    private Properties clientInfo = new Properties();
    private JdbcCompatibility compatibilityLevel;
    private boolean closed;

    private final AtomicBoolean warnedReadOnly = new AtomicBoolean();
    private final AtomicBoolean warnedTransactionIsolation = new AtomicBoolean();

    /**
     * Creates a new connection
     *
     * @throws SQLException Thrown if the arguments are invalid
     */
    public ConnectionImpl(QueryEngine queryEngine, JdbcCompatibility compatibilityLevel) throws SQLException {
        this.queryEngine = requireNonNull(queryEngine, "queryEngine");
        this.compatibilityLevel = or(compatibilityLevel, queryEngine.getDefaultCompatibilityLevel());
        this.metadata = queryEngine.getDatabaseMetaData(this);
    }

    @Override
    public ResourceManager getResources() {
        return resources;
    }

    /**
     * Gets the JDBC compatibility level that is in use, see
     * {@link JdbcCompatibility} for explanations
     *
     * @return Compatibility level
     */
    @Override
    public JdbcCompatibility getJdbcCompatibilityLevel() throws SQLException {
        checkClosed();
        return compatibilityLevel;
    }

    /**
     * Sets the JDBC compatibility level that is in use, see
     * {@link JdbcCompatibility} for explanations.
     * <p>
     * Changing the level may not effect existing open objects, behaviour in
     * this case will be implementation specific.
     * </p>
     *
     * @param compatibilityLevel Compatibility level
     */
    @Override
    public void setJdbcCompatibilityLevel(JdbcCompatibility compatibilityLevel) throws SQLException {
        checkClosed();
        this.compatibilityLevel = requireNonNull(compatibilityLevel, "compatibilityLevel");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return DataWorldConnection.class.equals(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        check(isWrapperFor(iface), "Not a wrapper for the desired interface");
        return iface.cast(this);
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        warnings.clear();
    }

    @Override
    public final void close() throws SQLException {
        if (closed) {
            return;
        }
        log.fine("Closing connection...");
        try {
            // Close statements that are still open
            resources.close();
        } catch (Exception e) {
            log.warning("Unexpected trying to close resources: " + e);
        } finally {
            closed = true;
            log.fine("Connection was closed");
        }
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public DataWorldStatement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, getHoldability());
    }

    @Override
    public DataWorldStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public final DataWorldStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkClosed();
        return new StatementImpl(queryEngine, this, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return true;
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return queryEngine.getCatalog();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return clientInfo;
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return clientInfo.getProperty(name);
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return metadata;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return TRANSACTION_NONE;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return warnings.get();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return true;
    }

    @Override
    public boolean isValid(int timeout) {
        return !closed;
    }

    @Override
    public String nativeSQL(String query) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public DataWorldCallableStatement prepareCall(String query) throws SQLException {
        return prepareCall(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, getHoldability());
    }

    @Override
    public DataWorldCallableStatement prepareCall(String query, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(query, resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public DataWorldCallableStatement prepareCall(String query, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkClosed();
        return new CallableStatementImpl(query, queryEngine, this, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public DataWorldPreparedStatement prepareStatement(String query) throws SQLException {
        return prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, getHoldability());
    }

    @Override
    public DataWorldPreparedStatement prepareStatement(String query, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        checkStatementGeneratedKeys(autoGeneratedKeys);
        checkSupported(autoGeneratedKeys == Statement.NO_GENERATED_KEYS, "Does not support auto-generated keys");
        return prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, getHoldability());
    }

    @Override
    public DataWorldPreparedStatement prepareStatement(String query, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public DataWorldPreparedStatement prepareStatement(String query, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public DataWorldPreparedStatement prepareStatement(String query, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(query, resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public DataWorldPreparedStatement prepareStatement(String query, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkClosed();
        return new PreparedStatementImpl(query, queryEngine, this, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        check(queryEngine.getCatalog().equals(catalog), "Catalog property is read-only");
    }

    @Override
    public void setClientInfo(Properties properties) {
        clientInfo = properties;
    }

    @Override
    public void setClientInfo(String name, String value) {
        clientInfo.put(name, value);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        checkResultSetHoldability(holdability);
        // Don't care what the holdability setting is since cursors aren't supported
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        if (!readOnly && warnedReadOnly.compareAndSet(false, true)) {
            warnings.add("Only read-only connections are supported");
        }
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        checkConnectionTransactionIsolation(level);
        if (level != Connection.TRANSACTION_NONE && warnedTransactionIsolation.compareAndSet(false, true)) {
            warnings.add("Transaction isolation level was set but transactions are not supported");
        }
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        check(queryEngine.getSchema().equals(schema), "Schema property is read-only");
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return queryEngine.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private void checkClosed() throws SQLException {
        check(!closed, "Connection is closed");
    }
}
