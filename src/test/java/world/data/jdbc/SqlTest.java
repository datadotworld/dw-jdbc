package world.data.jdbc;

import fi.iki.elonen.NanoHTTPD;
import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlTest {

    private static String lastUri;
    private static String resultResourceName = "hall_of_fame.json";
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

    @org.junit.Test
    public void test() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement()) {
            try (final ResultSet resultSet = statement.executeQuery("select * from HallOfFame order by yearid, playerID limit 10")) {
                ResultSetMetaData rsmd = resultSet.getMetaData();
                int columnsNumber = rsmd.getColumnCount();
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    System.out.print(rsmd.getColumnName(i));
                }
                System.out.println("");
                while (resultSet.next()) {
                    for (int i = 1; i <= columnsNumber; i++) {
                        if (i > 1) System.out.print(",  ");
                        String columnValue = resultSet.getString(i);
                        System.out.print(columnValue);
                    }
                    System.out.println("");
                }
            }
        }
        assertThat(lastUri).isEqualTo("http://localhost:3333/sql/dave/lahman-sabremetrics-dataset?query=select+*+from+HallOfFame+order+by+yearid%2C+playerID+limit+10");
    }
    @org.junit.Test
    public void testPrepared() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setInt(1, 3);
            try (final ResultSet resultSet = statement.executeQuery()) {
                ResultSetMetaData rsmd = resultSet.getMetaData();
                int columnsNumber = rsmd.getColumnCount();
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    System.out.print(rsmd.getColumnName(i));
                }
                System.out.println("");
                while (resultSet.next()) {
                    for (int i = 1; i <= columnsNumber; i++) {
                        if (i > 1) System.out.print(",  ");
                        String columnValue = resultSet.getString(i);
                        System.out.print(columnValue);
                    }
                    System.out.println("");
                }
            }
        }
        assertThat(lastUri).isEqualTo("http://localhost:3333/sql/dave/lahman-sabremetrics-dataset?query=select+*+from+HallOfFame+where+yearid+%3E+%3F+order+by+yearid%2C+playerID+limit+10&parameters=%24data_world_param0%3D%223%22%5E%5E%3Chttp%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23integer%3E");

    }

    @org.junit.Test(expected = SQLException.class)
    public void testIndexOutOfBounds() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setInt(1, 3);
            statement.setString(2, "foo");
        }
    }
    
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetArray() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setArray(1, null);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetAsciiStream() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setAsciiStream(1, null);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetAsciiStream2() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setAsciiStream(1, null, 3);
        }
    }
    
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetAsciiStream3() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setAsciiStream(1, null, 3L);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetBinaryStream() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setBinaryStream(1, null);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetBinaryStream2() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setBinaryStream(1, null, 3);
        }
    }
    
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetBinaryStream3() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setBinaryStream(1, null, 3L);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetCharacterStream() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setCharacterStream(1, null);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testBytes() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setBytes(1, null);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetCharacterStream2() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setCharacterStream(1, null, 3);
        }
    }
    
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetCharacterStream3() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setCharacterStream(1, null, 3L);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetBlob() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setBlob(1, (Blob)null);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetBlob2() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setBlob(1, (InputStream)null);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetBlob3() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setBlob(1, null, 3L);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetClob() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setClob(1, (Clob)null);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetClob2() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setClob(1, (Reader) null);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetClob3() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setClob(1, null, 3L);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetNCharacterStream() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setNCharacterStream(1, null);
        }
    }
    
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetNCharacterStream2() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setNCharacterStream(1,  null, 3L);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetNClob() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setNClob(1, (NClob)null);
        }
    }
    
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetNClob2() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setNClob(1, (Reader) null);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetNClob3() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setNClob(1, null, 3L);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetRef() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setRef(1, null);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetRowId() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setRowId(1, null);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSQLXML() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setSQLXML(1, null);
        }
    }

    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetDate() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setDate(1, null, null);
        }
    }

    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetTime() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setTime(1, null, null);
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetTimestamp() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setTimestamp(1, null, null);
        }
    }

    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetUnicodeStream() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where yearid > ? order by yearid, playerID limit 10")) {
            statement.setUnicodeStream(1, null, 3);
        }
    }

    @org.junit.Test
    public void testMetadataQueries() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement()) {
            try (final ResultSet resultSet = statement.executeQuery("select * from HallOfFame order by yearid, playerID limit 10")) {
                final ResultSetMetaData metaData = resultSet.getMetaData();
                assertThat(metaData.getColumnCount()).isEqualTo(10);
                assertThat(metaData.getColumnClassName(1)).isEqualTo("java.lang.String");
                assertThat(metaData.getColumnDisplaySize(1)).isEqualTo(Integer.MAX_VALUE);
                assertThat(metaData.getColumnLabel(1)).isEqualTo("playerID");
                assertThat(metaData.getColumnName(1)).isEqualTo("playerID");
                assertThat(metaData.getScale(1)).isEqualTo(0);
                assertThat(metaData.getPrecision(1)).isEqualTo(0);
                assertThat(metaData.getSchemaName(1)).isEqualTo("");
                assertThat(metaData.getTableName(1)).isEqualTo("");
                assertThat(metaData.isNullable(1)).isEqualTo(1);
                assertThat(metaData.isSigned(1)).isEqualTo(false);
                assertThat(metaData.isAutoIncrement(1)).isEqualTo(false);
                assertThat(metaData.isCaseSensitive(1)).isEqualTo(true);
                assertThat(metaData.isCurrency(1)).isEqualTo(false);
                assertThat(metaData.isDefinitelyWritable(1)).isEqualTo(false);
                assertThat(metaData.isReadOnly(1)).isEqualTo(true);
                assertThat(metaData.isSearchable(1)).isEqualTo(true);
                assertThat(metaData.isWritable(1)).isEqualTo(false);
                assertThat(metaData.getColumnType(1)).isEqualTo(-9);
                assertThat(metaData.getColumnTypeName(1)).isEqualTo("org.apache.jena.graph.Node");
                assertThat(metaData.getCatalogName(1)).isEqualTo("RDF");
            }
        }
    }
    @org.junit.Test(expected = SQLException.class)
    public void testMetadataQueryOutOfRange() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement()) {
            try (final ResultSet resultSet = statement.executeQuery("select * from HallOfFame order by yearid, playerID limit 10")) {
                final ResultSetMetaData metaData = resultSet.getMetaData();
                assertThat(metaData.getColumnClassName(11)).isEqualTo("java.lang.String");
            }
        }
    }

    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testMetadataIsWrapperFor() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement()) {
            try (final ResultSet resultSet = statement.executeQuery("select * from HallOfFame order by yearid, playerID limit 10")) {
              resultSet.getMetaData().isWrapperFor(Class.class);
            }
        }
    }
    @org.junit.Test(expected = SQLFeatureNotSupportedException.class)
    public void testMetadataUnwrap() throws Exception {

        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement()) {
            try (final ResultSet resultSet = statement.executeQuery("select * from HallOfFame order by yearid, playerID limit 10")) {
                resultSet.getMetaData().unwrap(Class.class);
            }
        }
    }
}
