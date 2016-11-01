package world.data.jdbc.results;

import fi.iki.elonen.NanoHTTPD;
import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Test;
import world.data.jdbc.JdbcCompatibility;
import world.data.jdbc.NanoHTTPDResource;
import world.data.jdbc.SparqlTest;
import world.data.jdbc.TestConfigSource;
import world.data.jdbc.statements.DataWorldStatement;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

public class GetValuesTest {
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

    @Test
    public void getBigDecimal() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select * from HallOfFame limit 10")) {
            resultSet.next();
            assertThat(resultSet.getBigDecimal(2)).isEqualTo(new BigDecimal(1936));
            assertThat(resultSet.getBigDecimal("yearid")).isEqualTo(new BigDecimal(1936));
            assertThat(resultSet.wasNull()).isFalse();
            assertThat(resultSet.getBigDecimal("null_col")).isNull();
            assertThat(resultSet.wasNull()).isTrue();
        }
    }

    @Test
    public void getBoolean1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select * from HallOfFame limit 10")) {
            resultSet.next();
            assertThat(resultSet.getBoolean(7)).isEqualTo(true);
            assertThat(resultSet.getBoolean("inducted")).isEqualTo(true);
            assertThat(resultSet.wasNull()).isFalse();
            assertThat(resultSet.getBoolean("null_col")).isEqualTo(false);
            assertThat(resultSet.wasNull()).isTrue();
        }
    }

    @Test
    public void getByte() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select * from HallOfFame limit 10")) {
            resultSet.next();
            assertThat(resultSet.getByte(6)).isEqualTo((byte) 55);
            assertThat(resultSet.getByte("votes")).isEqualTo((byte) 55);
            assertThat(resultSet.wasNull()).isFalse();
            assertThat(resultSet.getByte("null_col")).isEqualTo((byte) 0);
            assertThat(resultSet.wasNull()).isTrue();
        }
    }

    @Test
    public void getDate() throws Exception {

    }

    @Test
    public void getDate1() throws Exception {

    }

    @Test
    public void getDouble() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select * from HallOfFame limit 10")) {
            resultSet.next();
            assertThat(resultSet.getDouble(2)).isEqualTo(1936.0);
            assertThat(resultSet.getDouble("yearid")).isEqualTo(1936.0);
            assertThat(resultSet.wasNull()).isFalse();
            assertThat(resultSet.getDouble("null_col")).isEqualTo(0.0);
            assertThat(resultSet.wasNull()).isTrue();
        }
    }

    @Test
    public void getFloat() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select * from HallOfFame limit 10")) {
            resultSet.next();
            assertThat(resultSet.getFloat(2)).isEqualTo(1936.0F);
            assertThat(resultSet.getFloat("yearid")).isEqualTo(1936.0F);
            assertThat(resultSet.wasNull()).isFalse();
            assertThat(resultSet.getFloat("null_col")).isEqualTo(0.0F);
            assertThat(resultSet.wasNull()).isTrue();
        }
    }

    @Test
    public void getInt() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select * from HallOfFame limit 10")) {
            resultSet.next();
            assertThat(resultSet.getInt(2)).isEqualTo(1936);
            assertThat(resultSet.getInt("yearid")).isEqualTo(1936);
            assertThat(resultSet.wasNull()).isFalse();
            assertThat(resultSet.getInt("null_col")).isEqualTo(0);
            assertThat(resultSet.wasNull()).isTrue();
        }
    }

    @Test
    public void getLong() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select * from HallOfFame limit 10")) {
            resultSet.next();
            assertThat(resultSet.getLong(2)).isEqualTo(1936L);
            assertThat(resultSet.getLong("yearid")).isEqualTo(1936L);
            assertThat(resultSet.wasNull()).isFalse();
            assertThat(resultSet.getLong("null_col")).isEqualTo(0L);
            assertThat(resultSet.wasNull()).isTrue();
        }
    }

    @Test
    public void getNString() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select * from HallOfFame limit 10")) {
            resultSet.next();
            assertThat(resultSet.getNString(3)).isEqualTo("BBWAA");
            assertThat(resultSet.getNString("votedBy")).isEqualTo("BBWAA");
            assertThat(resultSet.wasNull()).isFalse();
            assertThat(resultSet.getNString("null_col")).isEqualTo(null);
            assertThat(resultSet.wasNull()).isTrue();
        }
    }

    @Test
    public void getObject() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select * from HallOfFame limit 10")) {
            resultSet.next();
            assertThat(resultSet.getObject(1)).isEqualTo("alexape01");
            assertThat(resultSet.getObject(2)).isEqualTo(1936L);
            assertThat(resultSet.getObject("yearid")).isEqualTo(1936L);
            assertThat(resultSet.wasNull()).isFalse();
            assertThat(resultSet.getObject("null_col")).isEqualTo(null);
            assertThat(resultSet.wasNull()).isTrue();
        }
    }

    @Test
    public void getObjectHiCompatability() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldStatement statement = (DataWorldStatement) connection.createStatement()) {
                statement.setJdbcCompatibilityLevel(JdbcCompatibility.HIGH);
                try (final ResultSet resultSet = statement.executeQuery("select * from HallOfFame limit 10")) {
                    resultSet.next();
                    assertThat(resultSet.getObject(1)).isEqualTo("alexape01");
                    assertThat(resultSet.getObject(2)).isEqualTo(1936L);
                    assertThat(resultSet.getObject("yearid")).isEqualTo(1936L);
                    assertThat(resultSet.wasNull()).isFalse();
                    assertThat(resultSet.getObject("null_col")).isEqualTo(null);
                    assertThat(resultSet.wasNull()).isTrue();
                }
            }
        }
    }

    @Test
    public void getShort() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select * from HallOfFame limit 10")) {
            resultSet.next();
            assertThat(resultSet.getShort(2)).isEqualTo((short) 1936);
            assertThat(resultSet.getShort("yearid")).isEqualTo((short) 1936);
            assertThat(resultSet.wasNull()).isFalse();
            assertThat(resultSet.getShort("null_col")).isEqualTo((short) 0);
            assertThat(resultSet.wasNull()).isTrue();
        }
    }

    @Test
    public void getString() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("select * from HallOfFame limit 10")) {
            resultSet.next();
            assertThat(resultSet.getString(3)).isEqualTo("BBWAA");
            assertThat(resultSet.getString("votedBy")).isEqualTo("BBWAA");
            assertThat(resultSet.wasNull()).isFalse();
            assertThat(resultSet.getString("null_col")).isEqualTo(null);
            assertThat(resultSet.wasNull()).isTrue();
        }
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


}