package world.data.jdbc.statements;

import fi.iki.elonen.NanoHTTPD;
import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Test;
import world.data.jdbc.JdbcCompatibility;
import world.data.jdbc.NanoHTTPDResource;
import world.data.jdbc.SparqlTest;
import world.data.jdbc.TestConfigSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class DataWorldPreparedStatementTest {
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
    public void addBatch() throws Exception {

    }

    @Test
    public void clearParameters() throws Exception {

    }

    @Test
    public void execute() throws Exception {

    }

    @Test
    public void executeQuery() throws Exception {

    }

    @Test
    public void executeUpdate() throws Exception {

    }

    @Test
    public void getMetaData() throws Exception {

    }

    @Test
    public void getParameterMetaData() throws Exception {

    }

    @Test
    public void setBigDecimal() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ? ")) {
                statement.setBigDecimal(1, new BigDecimal(3));
                assertThat(statement.formatParams()).isEqualTo("$data_world_param0=\"3\"^^<http://www.w3.org/2001/XMLSchema#decimal>");
            }
        }
    }

    @Test
    public void setBoolean() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ?  ")) {
                statement.setBoolean(1, true);
                assertThat(statement.formatParams()).isEqualTo("$data_world_param0=\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>");
            }
        }
    }

    @Test
    public void setDate() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ?  ")) {
                statement.setDate(1, new Date(1477433443000L));
                assertThat(statement.formatParams()).isEqualTo("$data_world_param0=\"2016-10-25T22:10:43+00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>");
            }
        }
    }

    @Test
    public void setDouble() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ? ")) {
                statement.setDouble(1, 3.0);
                assertThat(statement.formatParams()).isEqualTo("$data_world_param0=\"3.0\"^^<http://www.w3.org/2001/XMLSchema#double>");
            }
        }
    }

    @Test
    public void setFloat() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ? ")) {
                statement.setFloat(1, 3.0F);
                assertThat(statement.formatParams()).isEqualTo("$data_world_param0=\"3.0\"^^<http://www.w3.org/2001/XMLSchema#float>");
            }
        }
    }

    @Test
    public void setInt() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ? ")) {
                statement.setInt(1, 3);
                assertThat(statement.formatParams()).isEqualTo("$data_world_param0=\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>");
            }
        }
    }

    @Test
    public void setLong() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ? ")) {
                statement.setLong(1, 3L);
                assertThat(statement.formatParams()).isEqualTo("$data_world_param0=\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>");
            }
        }
    }

    @Test
    public void setNString() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ? ")) {
                statement.setNString(1, "foo");
                assertThat(statement.formatParams()).isEqualTo("$data_world_param0=\"foo\"");
            }
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNull() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ? ")) {
                statement.setNull(1, 1);
            }
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNull1() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ? ")) {
                statement.setNull(1, 1, "foo");
            }
        }
    }

    @Test
    public void setObject() throws Exception {

    }

    @Test
    public void setObject1() throws Exception {

    }

    @Test
    public void setObject2() throws Exception {

    }

    @Test
    public void setByte() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ? ")) {
                statement.setByte(1, (byte)4);
                assertThat(statement.formatParams()).isEqualTo("$data_world_param0=\"4\"^^<http://www.w3.org/2001/XMLSchema#byte>");
            }
        }
    }
    @Test
    public void setShort() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ? ")) {
                statement.setShort(1, (short)4);
                assertThat(statement.formatParams()).isEqualTo("$data_world_param0=\"4\"^^<http://www.w3.org/2001/XMLSchema#short>");
            }
        }
    }

    @Test
    public void setString() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ? ")) {
                statement.setString(1, "foo");
                assertThat(statement.formatParams()).isEqualTo("$data_world_param0=\"foo\"");
            }
        }
    }

    @Test
    public void setTime() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ?  ")) {
                statement.setTime(1, new Time(1477433443000L));
                assertThat(statement.formatParams()).isEqualTo("$data_world_param0=\"22:10:43+00:00\"^^<http://www.w3.org/2001/XMLSchema#time>");
            }
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setTimestamp() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties())) {
            try (final DataWorldPreparedStatement statement = (DataWorldPreparedStatement) connection.prepareStatement("select * from Fielding where yearid = ?  ")) {
                statement.setTimestamp(1, new Timestamp(1477433443000L));
            }
        }
    }

}