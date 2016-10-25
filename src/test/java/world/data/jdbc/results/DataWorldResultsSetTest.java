package world.data.jdbc.results;

import fi.iki.elonen.NanoHTTPD;
import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Test;
import world.data.jdbc.NanoHTTPDResource;
import world.data.jdbc.SparqlTest;
import world.data.jdbc.TestConfigSource;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class DataWorldResultsSetTest {
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
    
    @Test(expected = SQLFeatureNotSupportedException.class)
    public void isWrapperFor() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.isWrapperFor(Class.class); 
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void unwrap() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.unwrap(Class.class);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void cancelRowUpdates() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.cancelRowUpdates();
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void deleteRow() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.deleteRow();
        }
    }

    @Test
    public void getHoldability() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            assertThat(resultSet.getHoldability()).isEqualTo(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        }
    }

    @Test
    public void getConcurrency() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            assertThat(resultSet.getConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
        }
    }

    @Test
    public void getNode() throws Exception {

    }

    @Test
    public void getBigDecimal() throws Exception {

    }

    @Test
    public void getBigDecimal1() throws Exception {

    }

    @Test
    public void getBoolean() throws Exception {

    }

    @Test
    public void getBoolean1() throws Exception {

    }

    @Test
    public void getByte() throws Exception {

    }

    @Test
    public void getByte1() throws Exception {

    }

    @Test
    public void getDate() throws Exception {

    }

    @Test
    public void getDate1() throws Exception {

    }

    @Test
    public void getDouble() throws Exception {

    }

    @Test
    public void getDouble1() throws Exception {

    }

    @Test
    public void getFloat() throws Exception {

    }

    @Test
    public void getFloat1() throws Exception {

    }

    @Test
    public void getInt() throws Exception {

    }

    @Test
    public void getInt1() throws Exception {

    }

    @Test
    public void getLong() throws Exception {

    }

    @Test
    public void getLong1() throws Exception {

    }

    @Test
    public void getNString() throws Exception {

    }

    @Test
    public void getNString1() throws Exception {

    }

    @Test
    public void getObject() throws Exception {

    }

    @Test
    public void getObject1() throws Exception {

    }

    @Test
    public void getShort() throws Exception {

    }

    @Test
    public void getShort1() throws Exception {

    }

    @Test
    public void getString() throws Exception {

    }

    @Test
    public void getString1() throws Exception {

    }

    @Test
    public void getTime() throws Exception {

    }

    @Test
    public void getTime1() throws Exception {

    }

    @Test
    public void getTimestamp() throws Exception {

    }

    @Test
    public void getTimestamp1() throws Exception {

    }

    @Test
    public void getURL() throws Exception {

    }

    @Test
    public void getURL1() throws Exception {

    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getArray() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getArray(1);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getArray1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getArray("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getAsciiStream() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getAsciiStream(1);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getAsciiStream1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getAsciiStream("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getBigDecimal2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getBigDecimal("foo", 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getBigDecimal3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getBigDecimal(3, 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getBinaryStream() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getBinaryStream(3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getBinaryStream1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getBinaryStream("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getBlob() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getBlob("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getBlob1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getBlob(3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getBytes() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getBytes("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getBytes1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getBytes(3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getCharacterStream() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getCharacterStream("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getCharacterStream1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getCharacterStream(3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getClob() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getClob("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getClob1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getClob(3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getCursorName() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getCursorName();
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getDate2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getDate("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getDate3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getDate(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getNCharacterStream() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getNCharacterStream("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getNCharacterStream1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getNCharacterStream(3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getNClob() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getNClob("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getNClob1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getNClob(3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getObject2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getObject("foo", String.class);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getObject3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getObject(3, String.class);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getObject4() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getObject("foo", Collections.emptyMap());
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getObject5() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getObject(3, Collections.emptyMap());
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getRef() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getRef("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getRef1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getRef(3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getRowId() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getRowId("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getRowId1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getRowId(3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getSQLXML() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getSQLXML("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getSQLXML1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getSQLXML(3);
        }
    }

    @Test
    public void getStatement() throws Exception {

    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getTime2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getTime("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getTime3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getTime(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getTimestamp2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getTimestamp("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getTimestamp3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getTimestamp(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getUnicodeStream() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getUnicodeStream("foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getUnicodeStream1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.getUnicodeStream(3);
        }
    }

    @Test
    public void getWarnings() throws Exception {

    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void insertRow() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.insertRow();
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void moveToCurrentRow() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.moveToCurrentRow();
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void moveToInsertRow() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.moveToInsertRow();
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void previous() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.previous();
        }
    }

    @Test
    public void refreshRow() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.refreshRow();
        }
    }

    @Test
    public void rowDeleted() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            assertThat(resultSet.rowDeleted()).isFalse();
        }
    }

    @Test
    public void rowInserted() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            assertThat(resultSet.rowInserted()).isFalse();
        }
    }

    @Test
    public void rowUpdated() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            assertThat(resultSet.rowUpdated()).isFalse();
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateArray() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateArray(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateArray1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateArray("s", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateAsciiStream() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateAsciiStream(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateAsciiStream1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateAsciiStream(3, null, 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateAsciiStream2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateAsciiStream(3, null, 3L);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateAsciiStream3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateAsciiStream("foo", null, 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateAsciiStream4() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateAsciiStream("foo", null, 3L);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateAsciiStream5() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateAsciiStream("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBigDecimal() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBigDecimal("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBigDecimal1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBigDecimal(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBinaryStream() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBinaryStream(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBinaryStream1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBinaryStream(3, null, 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBinaryStream2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBinaryStream(3, null, 3L);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBinaryStream3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBinaryStream("foo", null, 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBinaryStream4() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBinaryStream("foo", null, 3L);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBinaryStream5() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBinaryStream("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBlob() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBlob(3, (InputStream) null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBlob1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBlob(3, (Blob)null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBlob2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBlob(3, null, 3L);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBlob3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBlob("foo", null, 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBlob4() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBlob("foo", (Blob)null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBlob5() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBlob("foo", (InputStream)null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBoolean() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBoolean("foo", true);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBoolean1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBoolean(3, true);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateByte() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateByte(3, (byte)2);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateByte1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateByte("foo", (byte)2);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBytes() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBytes("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBytes1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateBytes(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateCharacterStream() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateCharacterStream(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateCharacterStream1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateCharacterStream(3, null, 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateCharacterStream2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateCharacterStream(3, null, 3L);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateCharacterStream3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateCharacterStream("foo", null, 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateCharacterStream4() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateCharacterStream("foo", null, 3L);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateCharacterStream5() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateCharacterStream("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateClob() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateClob(3, (Reader) null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateClob1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateClob(3, (Clob)null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateClob2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateClob(3, null, 3L);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateClob3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateClob("foo", null, 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateClob4() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateClob("foo", (Clob)null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateClob5() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateClob("foo", (Reader)null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateDate() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateDate("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateDate1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateDate(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateDouble() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateDouble("foo", 3.0);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateDouble1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateDouble(3, 3.0);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateFloat() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateFloat("foo", 3.0f);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateFloat1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateFloat(3, 3.0f);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateInt() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateInt("foo", 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateInt1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateInt(3, 4);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateLong() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateLong("foo", 3L);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateLong1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateLong(3, 3L);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNCharacterStream() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNCharacterStream(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNCharacterStream1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNCharacterStream(3, null, 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNCharacterStream2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNCharacterStream(3, null, 3L);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNCharacterStream3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNCharacterStream("foo", null, 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNCharacterStream4() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNCharacterStream("foo", null, 3L);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNCharacterStream5() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNCharacterStream("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNClob() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNClob(3, (Reader) null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNClob1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNClob(3, (NClob)null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNClob2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNClob(3, null, 3L);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNClob3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNClob("foo", null, 3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNClob4() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNClob("foo", (NClob)null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNClob5() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNClob("foo", (Reader)null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNString() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNString(3, "foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNString1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNString("bar", "foo");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNull() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNull("bar");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNull1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateNull(3);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateObject() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateObject("bar", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateObject1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateObject("bar", null , 2);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateObject2() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateObject(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateObject3() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateObject(3, null, 2);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateRef() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateRef(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateRef1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateRef("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateRow() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateRow();
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateRowId() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateRowId(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateRowId1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateRowId("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateSQLXML() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateSQLXML(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateSQLXML1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateSQLXML("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateShort() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateShort(3, (short)2);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateShort1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateShort("foo", (short)2);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateString() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateString("foo", "bar");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateString1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateString(3, "bar");
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateTime() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateTime(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateTime1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateTime("foo", null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateTimestamp() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateTimestamp(3, null);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateTimestamp1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.}")) {
            resultSet.updateTimestamp("foo", null);
        }
    }

    public void wasNull() throws Exception {

    }


}