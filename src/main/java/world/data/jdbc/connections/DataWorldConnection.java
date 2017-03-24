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
package world.data.jdbc.connections;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.jena.jdbc.metadata.JenaMetadata;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import world.data.jdbc.DataWorldJdbcDriver;
import world.data.jdbc.JdbcCompatibility;
import world.data.jdbc.statements.DataWorldCallableStatement;
import world.data.jdbc.statements.DataWorldPreparedStatement;
import world.data.jdbc.statements.DataWorldStatement;
import world.data.jdbc.statements.SparqlStatementQueryBuilder;
import world.data.jdbc.statements.SqlStatementQueryBuilder;

import java.io.IOException;
import java.net.URI;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLRecoverableException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.util.Conditions.check;
import static world.data.jdbc.util.Conditions.checkSupported;

/**
 * Abstract base implementation of a Jena JDBC connection
 * <p>
 * Generally speaking this is a faithful implementation of a JDBC connection but
 * it also provides a couple of Jena JDBC specific features:
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
 * Command pre-processors are an extension mechanism designed to allow Jena JDBC
 * connections to cope with the fact that the tools consuming the API may be
 * completely unaware that we speak SPARQL rather than SQL. They allow for
 * manipulation of incoming command text as well as manipulation of the parsed
 * SPARQL queries and updates as desired.
 * </p>
 */
public class DataWorldConnection implements Connection {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataWorldConnection.class);

    static {
        ARQ.init();
    }

    /**
     * Constant for default cursor holdability for data.world JDBC connections
     */
    public static final int DEFAULT_HOLDABILITY = ResultSet.CLOSE_CURSORS_AT_COMMIT;
    /**
     * Constant for default auto-commit for data.world JDBC connections
     */
    private final static boolean DEFAULT_AUTO_COMMIT = true;
    /**
     * Constant for default transaction isolation level for data.world JDBC
     * connections
     */
    private final static int DEFAULT_ISOLATION_LEVEL = TRANSACTION_NONE;
    private final String queryService;
    private final String lang;
    private final CloseableHttpClient httpClient;
    private final List<HttpHost> preemptiveAuthHosts;
    private final DatabaseMetaData metadata;

    private Properties clientInfo = new Properties();
    private SQLWarning warnings;
    private JdbcCompatibility compatibilityLevel;
    private final List<Statement> statements = new ArrayList<>();
    private boolean closed;

    /**
     * Creates a new connection
     *
     * @throws SQLException Thrown if the arguments are invalid
     */
    public DataWorldConnection(String queryEndpoint, String lang, String token) throws SQLException {
        this.queryService = requireNonNull(queryEndpoint, "queryEndpoint");
        this.lang = requireNonNull(lang, "lang");
        this.compatibilityLevel = "sql".equals(lang) ? JdbcCompatibility.HIGH : JdbcCompatibility.DEFAULT;
        this.metadata = "sparql".equals(lang) ? new SparqlDatabaseMetaData(this) : new SqlDatabaseMetaData(this);

        Duration socketTimeout = Duration.ofSeconds(60);
        Duration keepAlive = Duration.ofSeconds(1);

        HttpHost queryHost = URIUtils.extractHost(URI.create(queryService));
        if (queryHost == null) {
            throw new IllegalArgumentException("Invalid query endpoint: " + queryService);
        }

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        if (token != null) {
            AuthScope authScope = new AuthScope(queryHost, AuthScope.ANY_REALM, "Bearer");
            credentialsProvider.setCredentials(authScope, new BearerCredentials(token));
            preemptiveAuthHosts = Collections.singletonList(queryHost);
        } else {
            preemptiveAuthHosts = Collections.emptyList();
        }

        this.httpClient = HttpClientBuilder.create()
                .setMaxConnPerRoute(5)
                .setMaxConnTotal(5)
                .setDefaultCredentialsProvider(credentialsProvider)
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                        .setRedirectsEnabled(false)
                        .setSocketTimeout((int) socketTimeout.toMillis())
                        .build())
                .setDefaultSocketConfig(SocketConfig.custom()
                        .setSoTimeout((int) socketTimeout.toMillis())
                        .build())
                .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy() {
                    @Override
                    public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                        long duration = super.getKeepAliveDuration(response, context);
                        return duration == -1L ? keepAlive.toMillis() : duration;
                    }
                })
                .setUserAgent(String.format("DwJdbc-%s/%s", lang, DataWorldJdbcDriver.VERSION))
                .build();
    }

    public QueryEngineHTTP createQueryExecution(Query q) throws SQLException {
        // Create basic execution
        QueryEngineHTTP exec = (QueryEngineHTTP) QueryExecutionFactory.sparqlService(queryService, q, httpClient);
        exec.setHttpContext(createHttpContext());
        return exec;
    }

    public HttpClientContext createHttpContext() {
        HttpClientContext context = HttpClientContext.create();
        BasicAuthCache authCache = new BasicAuthCache();
        preemptiveAuthHosts.forEach(host -> authCache.put(host, new BearerScheme()));
        context.setAuthCache(authCache);
        return context;
    }

    /**
     * Gets the JDBC compatibility level that is in use, see
     * {@link JdbcCompatibility} for explanations
     *
     * @return Compatibility level
     */
    public JdbcCompatibility getJdbcCompatibilityLevel() {
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
    public void setJdbcCompatibilityLevel(JdbcCompatibility compatibilityLevel) {
        this.compatibilityLevel = requireNonNull(compatibilityLevel, "compatibilityLevel");
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearWarnings() {
        this.warnings = null;
    }

    @Override
    public final void close() throws SQLException {
        if (closed) {
            return;
        }
        try {
            LOGGER.info("Closing connection...");
            // Close any open statements
            closeStatements();
        } finally {
            closeInternal();
            LOGGER.info("Connection was closed");
        }
    }

    private void closeStatements() throws SQLException {
        synchronized (statements) {
            if (statements.size() > 0) {
                LOGGER.info("Attempting to close " + statements.size() + " open statements");
                for (Statement stmt : statements) {
                    stmt.close();
                }
                LOGGER.info("All open statements were closed");
                statements.clear();
            }
        }
    }

    private void closeInternal() {
        try {
            this.httpClient.close();
        } catch (IOException e) {
            LOGGER.warn("Unexpected trying to close HTTP client.", e);
        } finally {
            closed = true;
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
    public final Statement createStatement() throws SQLException {
        checkClosed();
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public final Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return createStatement(resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public final Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        checkClosed();
        DataWorldStatement stmt = createStatementInternal(resultSetType, resultSetConcurrency);
        synchronized (statements) {
            statements.add(stmt);
        }
        return stmt;
    }

    private DataWorldStatement createStatementInternal(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        checkSupported(resultSetType == ResultSet.TYPE_FORWARD_ONLY, "data.world connections only support forward-scrolling result sets");
        checkSupported(resultSetConcurrency == ResultSet.CONCUR_READ_ONLY, "Remote endpoint backed connections only support read-only result sets");
        if ("sparql".equals(lang)) {
            return new DataWorldStatement(this, new SparqlStatementQueryBuilder());
        } else {
            return new DataWorldStatement(this, new SqlStatementQueryBuilder());
        }
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean getAutoCommit() {
        return DEFAULT_AUTO_COMMIT;
    }

    @Override
    public String getCatalog() {
        return JenaMetadata.DEFAULT_CATALOG;
    }

    @Override
    public Properties getClientInfo() {
        return clientInfo;
    }

    @Override
    public String getClientInfo(String name) {
        return clientInfo.getProperty(name);
    }

    @Override
    public int getHoldability() {
        return DEFAULT_HOLDABILITY;
    }

    @Override
    public DatabaseMetaData getMetaData() {
        return metadata;
    }

    @Override
    public int getTransactionIsolation() {
        return DEFAULT_ISOLATION_LEVEL;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() {
        return warnings;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public boolean isValid(int timeout) {
        return !closed;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        checkClosed();
        return createCalledStatementInternal(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return createCalledStatementInternal(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        checkClosed();
        return createCalledStatementInternal(sql, resultSetType, resultSetConcurrency);
    }

    private CallableStatement createCalledStatementInternal(String sparql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkSupported(resultSetType == ResultSet.TYPE_FORWARD_ONLY, "Remote endpoint backed connection do not support scroll sensitive result sets");
        checkSupported(resultSetConcurrency == ResultSet.CONCUR_READ_ONLY, "Remote endpoint backed connections only support read-only result sets");
        if ("sparql".equals(lang)) {
            return new DataWorldCallableStatement(sparql, this, new SparqlStatementQueryBuilder());
        } else {
            return new DataWorldCallableStatement(sparql, this, new SqlStatementQueryBuilder());
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return prepareStatement(sql, resultSetType, resultSetConcurrency, DEFAULT_HOLDABILITY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        checkClosed();
        return createPreparedStatementInternal(sql, resultSetType, resultSetConcurrency);
    }

    private PreparedStatement createPreparedStatementInternal(String sparql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkSupported(resultSetType == ResultSet.TYPE_FORWARD_ONLY, "Remote endpoint backed connection do not support scroll sensitive result sets");
        checkSupported(resultSetConcurrency == ResultSet.CONCUR_READ_ONLY, "Remote endpoint backed connections only support read-only result sets");
        if ("sparql".equals(lang)) {
            return new DataWorldPreparedStatement(sparql, this, new SparqlStatementQueryBuilder());
        } else {
            return new DataWorldPreparedStatement(sparql, this, new SqlStatementQueryBuilder());
        }
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
    public void setAutoCommit(boolean autoCommit) {
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLRecoverableException(String.format("%d is not a valid holdability setting", holdability));
        }
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        check(readOnly, "data.world does not support read/write connections");
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
        checkSupported(level == Connection.TRANSACTION_NONE, "Transactions are not supported for remote endpoint backed connections");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    //--- Java6/7 compatibility.
    @Override
    public void setSchema(String schema) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getSchema() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void abort(java.util.concurrent.Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(java.util.concurrent.Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private void checkClosed() throws SQLException {
        check(!closed, "Connection is closed");
    }
}
