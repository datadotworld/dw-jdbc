
package world.data.jdbc.results;

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

public class StreamedResultsTest {
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

    @Test(expected = SQLException.class)
    public void absoluteClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.close();
            assertThat(resultSet.absolute(9)).isEqualTo(1);
        }
    }

    @Test(expected = SQLException.class)
    public void absoluteNoMove() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            assertThat(resultSet.isBeforeFirst()).isTrue();
            assertThat(resultSet.isFirst()).isFalse();
            assertThat(resultSet.absolute(0)).isTrue();
        }
    }

    @Test
    public void absoluteFirst() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.next();
            assertThat(resultSet.absolute(1)).isTrue();
            assertThat(resultSet.absolute(1)).isTrue();
            assertThat(resultSet.getRow()).isEqualTo(1);
        }
    }

    @Test
    public void absoluteMoveForward() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            assertThat(resultSet.absolute(2)).isTrue();
            assertThat(resultSet.absolute(2)).isTrue();
            assertThat(resultSet.getRow()).isEqualTo(2);
        }
    }

    @Test(expected = SQLException.class)
    public void absoluteMoveBack() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            assertThat(resultSet.absolute(3)).isTrue();
            assertThat(resultSet.getRow()).isEqualTo(3);
            assertThat(resultSet.absolute(2));
        }
    }
    @Test
    public void absoluteMoveLast() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            assertThat(resultSet.absolute(-1)).isTrue();
            assertThat(resultSet.isLast()).isTrue();
            resultSet.next();
            assertThat(resultSet.isAfterLast()).isTrue();
        }
    }

    @Test
    public void absoluteMoveFurther() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            assertThat(resultSet.absolute(1000)).isFalse();
            assertThat(resultSet.isAfterLast()).isTrue();
        }
    }

    @Test(expected = SQLException.class)
    public void absoluteSecondLast() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            assertThat(resultSet.absolute(-2));
        }
    }

    @Test(expected = SQLException.class)
    public void absoluteAfterLastClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.close();
            resultSet.afterLast();
        }
    }
    @Test
    public void absoluteAfterLast() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.afterLast();
            assertThat(resultSet.isAfterLast()).isTrue();
            resultSet.afterLast();
            assertThat(resultSet.isAfterLast()).isTrue();
        }
    }

    @Test(expected = SQLException.class)
    public void absoluteBeforeFirstClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.close();
            resultSet.beforeFirst();
        }
    }

    @Test(expected = SQLException.class)
    public void absoluteBeforeFirstMoved() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.next();
            resultSet.beforeFirst();
        }
    }

    @Test
    public void absoluteBeforeFirstOk() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.beforeFirst();
        }
    }
    @Test(expected = SQLException.class)
    public void firstClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.close();
            resultSet.first();
        }
    }
    @Test(expected = SQLException.class)
    public void firstMoved() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.absolute(3);
            resultSet.first();
        }
    }

    @Test
    public void firstOk() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.next();
            resultSet.first();
        }
    }

    @Test
    public void getFetchDirectionSize() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            assertThat(resultSet.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
            assertThat(resultSet.getFetchSize()).isEqualTo(0);
            assertThat(resultSet.getType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
        }
    }

    @Test(expected = SQLException.class)
    public void isAfterLastClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.close();
            resultSet.isAfterLast();
        }
    }

    @Test(expected = SQLException.class)
    public void isBeforeFirstClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.close();
            resultSet.isBeforeFirst();
        }
    }

    @Test(expected = SQLException.class)
    public void isFirstClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.close();
            resultSet.isFirst();
        }
    }

    @Test(expected = SQLException.class)
    public void isLastClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.close();
            resultSet.isLast();
        }
    }
    @Test(expected = SQLException.class)
    public void lastClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.close();
            resultSet.last();
        }
    }

    @Test(expected = SQLException.class)
    public void nextClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.close();
            resultSet.next();
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setFetchSize() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.setFetchSize(0);
        }
    }
    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setFetchDirection() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.setFetchDirection(ResultSet.FETCH_REVERSE);
        }
    }
    @Test
    public void setFetchDirectionOk() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
        }
    }

    @Test(expected = SQLException.class)
    public void relativeClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.close();
            resultSet.relative(0);
        }
    }

    @Test
    public void relativeZero() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.relative(0);
        }
    }

    @Test(expected = SQLException.class)
    public void relativeNegative() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.relative(-2);
        }
    }
    @Test
    public void relativeShort() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            assertThat(resultSet.relative(3)).isTrue();
            assertThat(resultSet.getRow()).isEqualTo(3);
        }
    }

    @Test
    public void relativeLong() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            assertThat(resultSet.relative(10000)).isFalse();
        }
    }

}