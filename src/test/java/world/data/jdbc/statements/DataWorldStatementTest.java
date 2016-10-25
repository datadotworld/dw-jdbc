package world.data.jdbc.statements;

import fi.iki.elonen.NanoHTTPD;
import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Test;
import world.data.jdbc.JdbcCompatibility;
import world.data.jdbc.NanoHTTPDResource;
import world.data.jdbc.SparqlTest;
import world.data.jdbc.TestConfigSource;
import world.data.jdbc.connections.DataWorldConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

public class DataWorldStatementTest {
    private static String lastUri;
    private static String resultResourceName = "select.json";
    private static String resultMimeType = "application/json";

    @ClassRule
    public static final NanoHTTPDResource proxiedServer = new NanoHTTPDResource(3333) {
        @Override
        protected NanoHTTPD.Response serve(NanoHTTPD.IHTTPSession session) throws Exception {
            final String queryParameterString = session.getQueryParameterString();
            if (queryParameterString != null) {
                lastUri = "http://localhost:3333" + session.getUri() + '?' + queryParameterString;
            } else {
                lastUri = "http://localhost:3333" + session.getUri();
            }
            return newResponse(NanoHTTPD.Response.Status.OK, resultMimeType, IOUtils.toString(SparqlTest.class.getResourceAsStream("/" + resultResourceName)));
        }
    };

    @Test
    public void getJdbcCompatibilityLevel() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
                assertThat(statement.getJdbcCompatibilityLevel()).isEqualTo(JdbcCompatibility.MEDIUM);
            }
        }

    }

    @Test
    public void getJdbcCompatibilityInheritedLevel() throws Exception {
        try (final DataWorldConnection connection = (DataWorldConnection) DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            connection.setJdbcCompatibilityLevel(JdbcCompatibility.LOW);
            try (final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
                assertThat(statement.getJdbcCompatibilityLevel()).isEqualTo(JdbcCompatibility.LOW);
            }
        }

    }

    @Test
    public void setJdbcCompatibilityLevel() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
                statement.setJdbcCompatibilityLevel(JdbcCompatibility.HIGH + 3);
                assertThat(statement.getJdbcCompatibilityLevel()).isEqualTo(JdbcCompatibility.HIGH);
            }
        }
    }

    @Test
    public void getFetchDirection() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
                statement.setJdbcCompatibilityLevel(JdbcCompatibility.HIGH + 3);
                assertThat(statement.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
                assertThat(statement.getFetchSize()).isEqualTo(0);
                assertThat(statement.getMaxFieldSize()).isEqualTo(0);
                assertThat(statement.getMaxRows()).isEqualTo(0);
                assertThat(statement.getResultSetHoldability()).isEqualTo(ResultSet.CLOSE_CURSORS_AT_COMMIT);
                assertThat(statement.getResultSetConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
                assertThat(statement.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
            }
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getGeneratedKeys() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.getGeneratedKeys();
        }
    }

    @Test
    public void getWarnings() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.setFetchSize(100);
            assertThat((Throwable) statement.getWarnings()).isNotNull();
            assertThat((Iterable<Throwable>) statement.getWarnings()).isNotEmpty();
        }
    }

    @Test
    public void getClearWarnings() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.setFetchSize(100);
            assertThat((Throwable) statement.getWarnings()).isNotNull();
            assertThat((Iterable<Throwable>) statement.getWarnings()).isNotEmpty();
            statement.clearWarnings();
            assertThat((Throwable) statement.getWarnings()).isNull();
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void isWrapperFor() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.isWrapperFor(String.class);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void unwrap() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.unwrap(String.class);
        }
    }

    @Test
    public void addBatch() throws Exception {

    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void cancel() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.cancel();
        }
    }

    @Test
    public void execute() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            final ResultSet query = statement.executeQuery("select ?s where {?s ?p ?o.}");
            assertThat(query.isBeforeFirst()).isTrue();
            query.close();
        }
    }

    @Test(expected = SQLException.class)
    public void executeClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.close();
            final ResultSet query = statement.executeQuery("select ?s where {?s ?p ?o.}");
        }
    }

    @Test(expected = SQLException.class)
    public void execute1Closed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.close();
            statement.execute("select ?s where {?s ?p ?o.}", new int[0]);
        }
    }

    @Test(expected = SQLException.class)
    public void execute2Closed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.close();
            statement.execute("select ?s where {?s ?p ?o.}", new String[0]);
        }
    }

    @Test
    public void executeBatch() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.addBatch("select ?s where {?s ?p ?o.}");
            statement.addBatch("select ?o where {?s ?p ?o.}");
            final int[] results = statement.executeBatch();
            assertThat(results).hasSize(2);
            assertThat(statement.getResultSet()).isNotNull();
            assertThat(statement.getMoreResults()).isTrue();
            assertThat(statement.getMoreResults()).isFalse();
            assertThat(statement.getResultSet()).isNull();

        }
    }

    @Test(expected = SQLException.class)
    public void executeBatchClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.addBatch("select ?s where {?s ?p ?o.}");
            statement.addBatch("select ?o where {?s ?p ?o.}");
            statement.close();
            statement.executeBatch();
        }
    }

    @Test(expected = SQLException.class)
    public void getResultSetClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.addBatch("select ?s where {?s ?p ?o.}");
            statement.addBatch("select ?o where {?s ?p ?o.}");
            final int[] results = statement.executeBatch();
            assertThat(results).hasSize(2);
            statement.close();
            final ResultSet firstResultSet = statement.getResultSet();
        }
    }

    @Test(expected = SQLException.class)
    public void getMorResultsClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.addBatch("select ?s where {?s ?p ?o.}");
            statement.addBatch("select ?o where {?s ?p ?o.}");
            final int[] results = statement.executeBatch();
            assertThat(results).hasSize(2);
            assertThat(statement.getResultSet()).isNotNull();
            statement.close();
            assertThat(statement.getMoreResults()).isTrue();

        }
    }
    @Test
    public void clearBatch() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.addBatch("select ?s where {?s ?p ?o.}");
            statement.addBatch("select ?o where {?s ?p ?o.}");
            statement.clearBatch();
            final int[] results = statement.executeBatch();
            assertThat(results).hasSize(0);
        }
    }

    @Test
    public void close() throws Exception {

    }

    @Test
    public void setEscapeProcessing() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.setEscapeProcessing(true);//doesn't actually do anything
        }
    }

    @Test
    public void setFetchDirection() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setFetchDirectionFail() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.setFetchDirection(ResultSet.FETCH_REVERSE);
        }
    }

    @Test
    public void setMaxFieldSize() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.setMaxFieldSize(100);
            assertThat((Throwable) statement.getWarnings()).isNotNull();
            assertThat((Iterable<Throwable>) statement.getWarnings()).isNotEmpty();
        }
    }

    @Test
    public void setMaxRows() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.setMaxRows(100);
        }
    }

    @Test
    public void setPoolable() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.setPoolable(true);
            assertThat((Throwable) statement.getWarnings()).isNotNull();
            assertThat((Iterable<Throwable>) statement.getWarnings()).isNotEmpty();
        }
    }

    @Test
    public void setQueryTimeout() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.setQueryTimeout(300);
            assertThat(statement.getQueryTimeout()).isEqualTo(300);
            statement.setQueryTimeout(-300);
            assertThat(statement.getQueryTimeout()).isEqualTo(0);
        }
    }

    @Test
    public void isCloseOnCompletion() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            assertThat(statement.isCloseOnCompletion()).isFalse();
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void closeOnCompletion() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.closeOnCompletion();
        }
    }

    @Test
    public void isPoolable() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            assertThat(statement.isPoolable()).isTrue();
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCursorName() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.setCursorName("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeUpdate() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.executeUpdate("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeUpdate1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.executeUpdate("foo", new int[0]);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeUpdate2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.executeUpdate("foo", new String[0]);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeUpdate3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.executeUpdate("foo", 3);
        }
    }

    @Test
    public void multipleWarnings() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.setMaxFieldSize(100);
            statement.setMaxRows(100);
            assertThat((Throwable) statement.getWarnings()).isNotNull();
            assertThat((Throwable) statement.getWarnings().getNextWarning()).isNotNull();
            assertThat((Iterable<Throwable>) statement.getWarnings()).hasSize(2);
        }
    }
    @Test
    public void getUpdateCount() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            assertThat(statement.getUpdateCount()).isEqualTo(-1);
        }
    }


    @Test
    public void getMoreResultsCloseCurrent() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.addBatch("select ?s where {?s ?p ?o.}");
            statement.addBatch("select ?o where {?s ?p ?o.}");
            final int[] results = statement.executeBatch();
            assertThat(results).hasSize(2);
            assertThat(statement.getResultSet()).isNotNull();
            assertThat(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)).isTrue();
            assertThat(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)).isFalse();
            assertThat(statement.getResultSet()).isNull();

        }
    }
    @Test
    public void getMoreResultsKeepCurrent() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.addBatch("select ?s where {?s ?p ?o.}");
            statement.addBatch("select ?o where {?s ?p ?o.}");
            final int[] results = statement.executeBatch();
            assertThat(results).hasSize(2);
            assertThat(statement.getResultSet()).isNotNull();
            assertThat(statement.getMoreResults(Statement.KEEP_CURRENT_RESULT)).isTrue();
            assertThat(statement.getMoreResults(Statement.KEEP_CURRENT_RESULT)).isFalse();
            assertThat(statement.getResultSet()).isNull();

        }
    }

    @Test
    public void getMoreResultsCloseAll() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
            statement.addBatch("select ?s where {?s ?p ?o.}");
            statement.addBatch("select ?o where {?s ?p ?o.}");
            final int[] results = statement.executeBatch();
            assertThat(results).hasSize(2);
            assertThat(statement.getResultSet()).isNotNull();
            assertThat(statement.getMoreResults(Statement.CLOSE_ALL_RESULTS)).isTrue();
            assertThat(statement.getMoreResults(Statement.CLOSE_ALL_RESULTS)).isFalse();
            assertThat(statement.getResultSet()).isNull();

        }
    }

}