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
package world.data.jdbc;

import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.NanoHTTPD.Method;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import world.data.jdbc.testing.NanoHTTPDHandler;
import world.data.jdbc.testing.NanoHTTPDResource;
import world.data.jdbc.testing.SqlHelper;
import world.data.jdbc.testing.Utils;
import world.data.jdbc.vocab.Xsd;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;
import static world.data.jdbc.testing.MoreAssertions.assertSQLFeatureNotSupported;

public class SqlTest {

    private static NanoHTTPDHandler lastBackendRequest;
    private static final String resultResourceName = "/hall_of_fame.json";
    private static final String resultMimeType = Utils.TYPE_SPARQL_RESULTS;

    @ClassRule
    public static final NanoHTTPDResource proxiedServer = new NanoHTTPDResource(3333) {
        @Override
        protected NanoHTTPD.Response serve(NanoHTTPD.IHTTPSession session) throws Exception {
            String authorization = session.getHeaders().get("authorization");
            if (!"Bearer access-token".equals(authorization)) {
                return newResponse(NanoHTTPD.Response.Status.UNAUTHORIZED, "text/plain", "Missing or incorrect password");
            }
            NanoHTTPDHandler.invoke(session, lastBackendRequest);
            String body = IOUtils.toString(getClass().getResourceAsStream(resultResourceName), UTF_8);
            return newResponse(NanoHTTPD.Response.Status.OK, resultMimeType, body);
        }
    };

    @Rule
    public final SqlHelper sql = new SqlHelper();

    @Before
    public void setup() {
        lastBackendRequest = mock(NanoHTTPDHandler.class);
    }

    private DataWorldPreparedStatement samplePreparedStatement() throws SQLException {
        DataWorldConnection connection = sql.connect();
        return sql.prepareStatement(connection, "select * from HallOfFame where yearid > ? order by yearid, playerID limit 10");
    }

    private ResultSet sampleResultSet() throws SQLException {
        DataWorldStatement statement = sql.createStatement(sql.connect());
        return sql.executeQuery(statement, "select * from HallOfFame order by yearid, playerID limit 10");
    }

    @Test
    public void test() throws Exception {
        DataWorldStatement statement = sql.createStatement(sql.connect());
        Utils.dumpToStdout(sql.executeQuery(statement, "select * from HallOfFame order by yearid, playerID limit 10 "));
        verify(lastBackendRequest).handle(Method.POST, sql.urlPath(), null, Utils.TYPE_FORM_URLENCODED,
                Utils.queryParam("query", "select * from HallOfFame order by yearid, playerID limit 10 "));
    }

    @Test
    public void testPrepared() throws Exception {
        DataWorldConnection connection = sql.connect();
        DataWorldPreparedStatement statement = sql.prepareStatement(connection, "select * from HallOfFame where yearid > ? order by yearid, playerID limit 10");
        statement.setInt(1, 3);
        Utils.dumpToStdout(sql.executeQuery(statement));
        verify(lastBackendRequest).handle(Method.POST, sql.urlPath(), null, Utils.TYPE_FORM_URLENCODED, String.join("&",
                Utils.queryParam("query", "select * from HallOfFame where yearid > ? order by yearid, playerID limit 10"),
                Utils.queryParam("$data_world_param0", "\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>")));
    }

    @Test
    public void testIndexOutOfBounds() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setInt(1, 3);
        assertSQLException(() -> statement.setString(2, "foo"));
    }

    @Test
    public void testMetadataQueries() throws Exception {
        DataWorldStatement statement = sql.createStatement(sql.connect());
        ResultSet resultSet = sql.executeQuery(statement, "select * from HallOfFame order by yearid, playerID limit 10");
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertThat(metaData.getColumnCount()).isEqualTo(10);
        assertThat(metaData.getColumnClassName(1)).isEqualTo("java.lang.String");
        assertThat(metaData.getColumnDisplaySize(1)).isEqualTo(Integer.MAX_VALUE);
        assertThat(metaData.getColumnLabel(1)).isEqualTo("playerID");
        assertThat(metaData.getColumnName(1)).isEqualTo("playerID");
        assertThat(metaData.getScale(1)).isEqualTo(0);
        assertThat(metaData.getPrecision(1)).isEqualTo(Integer.MAX_VALUE);
        assertThat(metaData.getSchemaName(1)).isEqualTo("lahman-sabremetrics-dataset");
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
        assertThat(metaData.getColumnClassName(1)).isEqualTo(String.class.getName());
        assertThat(metaData.getColumnTypeName(1)).isEqualTo(Xsd.STRING.getIri());
        assertThat(metaData.getCatalogName(1)).isEqualTo("dave");
    }

    @Test
    public void testMetadataQueryOutOfRange() throws Exception {
        ResultSetMetaData metaData = sampleResultSet().getMetaData();
        assertSQLException(() -> metaData.getColumnClassName(11));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testAllNotSupported() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        assertSQLFeatureNotSupported(() -> statement.setArray(1, null));
        assertSQLFeatureNotSupported(() -> statement.setAsciiStream(1, null));
        assertSQLFeatureNotSupported(() -> statement.setAsciiStream(1, null, 3));
        assertSQLFeatureNotSupported(() -> statement.setAsciiStream(1, null, 3L));
        assertSQLFeatureNotSupported(() -> statement.setBinaryStream(1, null));
        assertSQLFeatureNotSupported(() -> statement.setBinaryStream(1, null, 3));
        assertSQLFeatureNotSupported(() -> statement.setBinaryStream(1, null, 3L));
        assertSQLFeatureNotSupported(() -> statement.setBlob(1, (Blob) null));
        assertSQLFeatureNotSupported(() -> statement.setBlob(1, (InputStream) null));
        assertSQLFeatureNotSupported(() -> statement.setBlob(1, null, 3L));
        assertSQLFeatureNotSupported(() -> statement.setBytes(1, null));
        assertSQLFeatureNotSupported(() -> statement.setCharacterStream(1, null));
        assertSQLFeatureNotSupported(() -> statement.setCharacterStream(1, null, 3));
        assertSQLFeatureNotSupported(() -> statement.setCharacterStream(1, null, 3L));
        assertSQLFeatureNotSupported(() -> statement.setClob(1, (Clob) null));
        assertSQLFeatureNotSupported(() -> statement.setClob(1, (Reader) null));
        assertSQLFeatureNotSupported(() -> statement.setClob(1, null, 3L));
        assertSQLFeatureNotSupported(() -> statement.setDate(1, null, null));
        assertSQLFeatureNotSupported(() -> statement.setNCharacterStream(1, null));
        assertSQLFeatureNotSupported(() -> statement.setNCharacterStream(1, null, 3L));
        assertSQLFeatureNotSupported(() -> statement.setNClob(1, (NClob) null));
        assertSQLFeatureNotSupported(() -> statement.setNClob(1, (Reader) null));
        assertSQLFeatureNotSupported(() -> statement.setNClob(1, null, 3L));
        assertSQLFeatureNotSupported(() -> statement.setRef(1, null));
        assertSQLFeatureNotSupported(() -> statement.setRowId(1, null));
        assertSQLFeatureNotSupported(() -> statement.setSQLXML(1, null));
        assertSQLFeatureNotSupported(() -> statement.setTime(1, null, null));
        assertSQLFeatureNotSupported(() -> statement.setTimestamp(1, null, null));
        assertSQLFeatureNotSupported(() -> statement.setUnicodeStream(1, null, 3));

        ResultSetMetaData md = sampleResultSet().getMetaData();
        assertSQLFeatureNotSupported(() -> md.isWrapperFor(Class.class));
        assertSQLFeatureNotSupported(() -> md.unwrap(Class.class));
    }
}
