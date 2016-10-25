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
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class SelectResultsTest {
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
    public void findColumn() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            assertThat(resultSet.findColumn("s")).isEqualTo(1);
        }
    }

    @Test(expected = SQLException.class)
    public void findColumnUnknown() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            assertThat(resultSet.findColumn("q"));
        }
    }

    @Test(expected = SQLException.class)
    public void findColumnClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.close();
            assertThat(resultSet.findColumn("s"));
        }
    }

    @Test(expected = SQLException.class)
    public void getNotAtRow() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            assertThat(resultSet.getURL(1)).isEqualTo(1);
        }
    }


    @Test(expected = SQLException.class)
    public void getUrClosedl() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.next();
            resultSet.close();
            assertThat(resultSet.getURL(1)).isEqualTo(1);
        }
    }

    @Test(expected = SQLException.class)
    public void getUrlOOBE() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.next();
            assertThat(resultSet.getURL(5)).isEqualTo(1);
        }
    }

    @Test
    public void getUrl() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.next();
            assertThat(resultSet.getURL(1).toString()).isEqualTo("http://data.world/user8/lahman-baseball/");
        }
    }

    @Test
    public void getUrlByName() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.next();
            assertThat(resultSet.getURL("s").toString()).isEqualTo("http://data.world/user8/lahman-baseball/");
        }
    }

    @Test(expected = SQLException.class)
    public void getUrlBadName() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            resultSet.next();
            assertThat(resultSet.getURL("Q").toString()).isEqualTo("http://data.world/user8/lahman-baseball/");
        }
    }


    @Test
    public void getMetaData() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
            assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(3);
        }
    }
    @Test
    public void getMetaDataLow() throws Exception {
        try (final DataWorldConnection connection =
                     (DataWorldConnection) DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            connection.setJdbcCompatibilityLevel(JdbcCompatibility.LOW);
            try (final Statement statement = connection.createStatement()) {
                try (final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
                    assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(3);
                }
            }
        }
    }

    @Test
    public void getMetaDataHigh() throws Exception {
        try (final DataWorldConnection connection =
                     (DataWorldConnection) DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            connection.setJdbcCompatibilityLevel(JdbcCompatibility.HIGH);
            try (final Statement statement = connection.createStatement()) {
                try (final ResultSet resultSet = statement.executeQuery("select ?s where {?s ?p ?o.}")) {
                    assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(3);
                }
            }
        }
    }


}