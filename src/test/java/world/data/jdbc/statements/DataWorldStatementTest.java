/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package world.data.jdbc.statements;

import fi.iki.elonen.NanoHTTPD;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import world.data.jdbc.JdbcCompatibility;
import world.data.jdbc.connections.DataWorldConnection;
import world.data.jdbc.testing.NanoHTTPDHandler;
import world.data.jdbc.testing.NanoHTTPDResource;
import world.data.jdbc.testing.SparqlHelper;

import java.sql.ResultSet;
import java.sql.Statement;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;
import static world.data.jdbc.testing.MoreAssertions.assertSQLFeatureNotSupported;

public class DataWorldStatementTest {
    private static NanoHTTPDHandler lastBackendRequest;
    private static final String resultResourceName = "/select.json";
    private static final String resultMimeType = "application/json";

    @ClassRule
    public static final NanoHTTPDResource proxiedServer = new NanoHTTPDResource(3333) {
        @Override
        protected NanoHTTPD.Response serve(NanoHTTPD.IHTTPSession session) throws Exception {
            NanoHTTPDHandler.invoke(session, lastBackendRequest);
            String body = IOUtils.toString(getClass().getResourceAsStream(resultResourceName), UTF_8);
            return newResponse(NanoHTTPD.Response.Status.OK, resultMimeType, body);
        }
    };

    @Rule
    public final SparqlHelper sparql = new SparqlHelper();

    @Before
    public void setup() {
        lastBackendRequest = mock(NanoHTTPDHandler.class);
    }

    @Test
    public void getJdbcCompatibilityLevel() throws Exception {
        DataWorldStatement statement = sparql.createStatement(sparql.connect());
        assertThat(statement.getJdbcCompatibilityLevel()).isEqualTo(JdbcCompatibility.MEDIUM);
    }

    @Test
    public void getJdbcCompatibilityInheritedLevel() throws Exception {
        DataWorldConnection connection = sparql.connect();
        connection.setJdbcCompatibilityLevel(JdbcCompatibility.LOW);
        DataWorldStatement statement = sparql.createStatement(connection);
        assertThat(statement.getJdbcCompatibilityLevel()).isEqualTo(JdbcCompatibility.LOW);
    }

    @Test
    public void setJdbcCompatibilityLevel() throws Exception {
        DataWorldConnection connection = sparql.connect();
        connection.setJdbcCompatibilityLevel(JdbcCompatibility.HIGH + 3);
        DataWorldStatement statement = sparql.createStatement(connection);
        assertThat(statement.getJdbcCompatibilityLevel()).isEqualTo(JdbcCompatibility.HIGH);
    }

    @Test
    public void getFetchDirection() throws Exception {
        DataWorldStatement statement = sparql.createStatement(sparql.connect());
        statement.setJdbcCompatibilityLevel(JdbcCompatibility.HIGH + 3);
        assertThat(statement.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
        assertThat(statement.getFetchSize()).isEqualTo(0);
        assertThat(statement.getMaxFieldSize()).isEqualTo(0);
        assertThat(statement.getMaxRows()).isEqualTo(0);
        assertThat(statement.getResultSetHoldability()).isEqualTo(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        assertThat(statement.getResultSetConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
        assertThat(statement.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
    }

    @Test
    public void getWarnings() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.setFetchSize(100);
        assertThat((Throwable) statement.getWarnings()).isNotNull();
        assertThat((Iterable<Throwable>) statement.getWarnings()).isNotEmpty();
    }

    @Test
    public void getClearWarnings() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.setFetchSize(100);
        assertThat((Throwable) statement.getWarnings()).isNotNull();
        assertThat((Iterable<Throwable>) statement.getWarnings()).isNotEmpty();
        statement.clearWarnings();
        assertThat((Throwable) statement.getWarnings()).isNull();
    }

    @Test
    public void addBatch() throws Exception {

    }

    @Test
    public void execute() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        ResultSet query = statement.executeQuery("select ?s where {?s ?p ?o.}");
        assertThat(query.isBeforeFirst()).isTrue();
        query.close();
    }

    @Test
    public void executeBatch() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.addBatch("select ?s where {?s ?p ?o.}");
        statement.addBatch("select ?o where {?s ?p ?o.}");
        int[] results = statement.executeBatch();
        assertThat(results).hasSize(2);
        assertThat(statement.getResultSet()).isNotNull();
        assertThat(statement.getMoreResults()).isTrue();
        assertThat(statement.getMoreResults()).isFalse();
        assertThat(statement.getResultSet()).isNull();

    }

    @Test
    public void executeBatchClosed() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.addBatch("select ?s where {?s ?p ?o.}");
        statement.addBatch("select ?o where {?s ?p ?o.}");
        statement.close();
        assertSQLException(statement::executeBatch);
    }

    @Test
    public void getResultSetClosed() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.addBatch("select ?s where {?s ?p ?o.}");
        statement.addBatch("select ?o where {?s ?p ?o.}");
        int[] results = statement.executeBatch();
        assertThat(results).hasSize(2);
        statement.close();
        assertSQLException(statement::getResultSet);
    }

    @Test
    public void getMoreResultsClosed() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.addBatch("select ?s where {?s ?p ?o.}");
        statement.addBatch("select ?o where {?s ?p ?o.}");
        int[] results = statement.executeBatch();
        assertThat(results).hasSize(2);
        assertThat(statement.getResultSet()).isNotNull();
        statement.close();
        assertSQLException(statement::getMoreResults);
    }

    @Test
    public void clearBatch() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.addBatch("select ?s where {?s ?p ?o.}");
        statement.addBatch("select ?o where {?s ?p ?o.}");
        statement.clearBatch();
        int[] results = statement.executeBatch();
        assertThat(results).hasSize(0);
    }

    @Test
    public void close() throws Exception {

    }

    @Test
    public void setEscapeProcessing() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.setEscapeProcessing(true);//doesn't actually do anything
    }

    @Test
    public void setFetchDirection() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
    }

    @Test
    public void setMaxFieldSize() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.setMaxFieldSize(100);
        assertThat((Throwable) statement.getWarnings()).isNotNull();
        assertThat((Iterable<Throwable>) statement.getWarnings()).isNotEmpty();
    }

    @Test
    public void setMaxRows() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.setMaxRows(100);
    }

    @Test
    public void setPoolable() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.setPoolable(true);
        assertThat((Throwable) statement.getWarnings()).isNotNull();
        assertThat((Iterable<Throwable>) statement.getWarnings()).isNotEmpty();
    }

    @Test
    public void setQueryTimeout() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.setQueryTimeout(300);
        assertThat(statement.getQueryTimeout()).isEqualTo(300);
        statement.setQueryTimeout(-300);
        assertThat(statement.getQueryTimeout()).isEqualTo(0);
    }

    @Test
    public void isCloseOnCompletion() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        assertThat(statement.isCloseOnCompletion()).isFalse();
    }

    @Test
    public void isPoolable() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        assertThat(statement.isPoolable()).isTrue();
    }

    @Test
    public void multipleWarnings() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.setMaxFieldSize(100);
        statement.setMaxRows(100);
        assertThat((Throwable) statement.getWarnings()).isNotNull();
        assertThat((Throwable) statement.getWarnings().getNextWarning()).isNotNull();
        assertThat((Iterable<Throwable>) statement.getWarnings()).hasSize(2);
    }

    @Test
    public void getUpdateCount() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        assertThat(statement.getUpdateCount()).isEqualTo(-1);
    }

    @Test
    public void getMoreResultsCloseCurrent() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.addBatch("select ?s where {?s ?p ?o.}");
        statement.addBatch("select ?o where {?s ?p ?o.}");
        int[] results = statement.executeBatch();
        assertThat(results).hasSize(2);
        assertThat(statement.getResultSet()).isNotNull();
        assertThat(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)).isTrue();
        assertThat(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)).isFalse();
        assertThat(statement.getResultSet()).isNull();

    }

    @Test
    public void getMoreResultsKeepCurrent() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.addBatch("select ?s where {?s ?p ?o.}");
        statement.addBatch("select ?o where {?s ?p ?o.}");
        int[] results = statement.executeBatch();
        assertThat(results).hasSize(2);
        assertThat(statement.getResultSet()).isNotNull();
        assertThat(statement.getMoreResults(Statement.KEEP_CURRENT_RESULT)).isTrue();
        assertThat(statement.getMoreResults(Statement.KEEP_CURRENT_RESULT)).isFalse();
        assertThat(statement.getResultSet()).isNull();
    }

    @Test
    public void getMoreResultsCloseAll() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.addBatch("select ?s where {?s ?p ?o.}");
        statement.addBatch("select ?o where {?s ?p ?o.}");
        int[] results = statement.executeBatch();
        assertThat(results).hasSize(2);
        assertThat(statement.getResultSet()).isNotNull();
        assertThat(statement.getMoreResults(Statement.CLOSE_ALL_RESULTS)).isTrue();
        assertThat(statement.getMoreResults(Statement.CLOSE_ALL_RESULTS)).isFalse();
        assertThat(statement.getResultSet()).isNull();
    }

    @Test
    public void testAllClosed() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        statement.close();
        assertSQLException(() -> statement.execute("select ?s where {?s ?p ?o.}", new String[0]));
        assertSQLException(() -> statement.execute("select ?s where {?s ?p ?o.}", new int[0]));
        assertSQLException(statement::executeBatch);
        assertSQLException(() -> statement.executeQuery("select ?s where {?s ?p ?o.}"));
        assertSQLException(statement::getMoreResults);
        assertSQLException(statement::getResultSet);
    }

    @Test
    public void testAllNotSupported() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        assertSQLFeatureNotSupported(statement::cancel);
        assertSQLFeatureNotSupported(statement::closeOnCompletion);
        assertSQLFeatureNotSupported(() -> statement.executeUpdate("foo"));
        assertSQLFeatureNotSupported(() -> statement.executeUpdate("foo", 3));
        assertSQLFeatureNotSupported(() -> statement.executeUpdate("foo", new String[0]));
        assertSQLFeatureNotSupported(() -> statement.executeUpdate("foo", new int[0]));
        assertSQLFeatureNotSupported(statement::getGeneratedKeys);
        assertSQLFeatureNotSupported(() -> statement.isWrapperFor(String.class));
        assertSQLFeatureNotSupported(() -> statement.setCursorName("foo"));
        assertSQLFeatureNotSupported(() -> statement.setFetchDirection(ResultSet.FETCH_REVERSE));
        assertSQLFeatureNotSupported(() -> statement.unwrap(String.class));
    }
}
