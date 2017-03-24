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
import world.data.jdbc.testing.NanoHTTPDHandler;
import world.data.jdbc.testing.NanoHTTPDResource;
import world.data.jdbc.testing.SqlHelper;

import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static world.data.jdbc.testing.MoreAssertions.assertSQLFeatureNotSupported;

public class PreparedStatementTest {
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
    public final SqlHelper sql = new SqlHelper();

    @Before
    public void setup() {
        lastBackendRequest = mock(NanoHTTPDHandler.class);
    }

    private PreparedStatement samplePreparedStatement() throws SQLException {
        return sql.prepareStatement(sql.connect(), "select * from Fielding where yearid = ?");
    }

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
        PreparedStatement statement = samplePreparedStatement();
        statement.setBigDecimal(1, new BigDecimal(3));
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"3\"^^<http://www.w3.org/2001/XMLSchema#decimal>")));
    }

    @Test
    public void setBoolean() throws Exception {
        PreparedStatement statement = samplePreparedStatement();
        statement.setBoolean(1, true);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>")));
    }

    @Test
    public void setByte() throws Exception {
        PreparedStatement statement = samplePreparedStatement();
        statement.setByte(1, (byte) 4);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"4\"^^<http://www.w3.org/2001/XMLSchema#byte>")));
    }

    @Test
    public void setDate() throws Exception {
        PreparedStatement statement = samplePreparedStatement();
        statement.setDate(1, new Date(1477433443000L));
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"2016-10-25T22:10:43Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>")));
    }

    @Test
    public void setDouble() throws Exception {
        PreparedStatement statement = samplePreparedStatement();
        statement.setDouble(1, 3.0);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"3.0\"^^<http://www.w3.org/2001/XMLSchema#double>")));
    }

    @Test
    public void setFloat() throws Exception {
        PreparedStatement statement = samplePreparedStatement();
        statement.setFloat(1, 3.0F);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"3.0\"^^<http://www.w3.org/2001/XMLSchema#float>")));
    }

    @Test
    public void setInt() throws Exception {
        PreparedStatement statement = samplePreparedStatement();
        statement.setInt(1, 3);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>")));
    }

    @Test
    public void setLong() throws Exception {
        PreparedStatement statement = samplePreparedStatement();
        statement.setLong(1, 3L);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>")));
    }

    @Test
    public void setNString() throws Exception {
        PreparedStatement statement = samplePreparedStatement();
        statement.setNString(1, "foo");
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"foo\"")));
    }

    @Test
    public void setNull() throws Exception {
        PreparedStatement statement = samplePreparedStatement();
        statement.setNull(1, Types.VARCHAR);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), eq("query=select+*+from+Fielding+where+yearid+%3D+%3F"));
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
    public void setShort() throws Exception {
        PreparedStatement statement = samplePreparedStatement();
        statement.setShort(1, (short) 4);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"4\"^^<http://www.w3.org/2001/XMLSchema#short>")));
    }

    @Test
    public void setString() throws Exception {
        PreparedStatement statement = samplePreparedStatement();
        statement.setString(1, "foo");
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"foo\"")));
    }

    @Test
    public void setTime() throws Exception {
        PreparedStatement statement = samplePreparedStatement();
        statement.setTime(1, new Time(1477433443000L));
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"22:10:43Z\"^^<http://www.w3.org/2001/XMLSchema#time>")));
    }


    @Test
    public void testNull() throws Exception {
        PreparedStatement statement = sql.prepareStatement(sql.connect(), "select * from Fielding where yearid in (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        int index = 0;
        statement.setString(++index, "not-null");
        statement.setBigDecimal(++index, null);
        statement.setDate(++index, null);
        statement.setNString(++index, null);
        statement.setNull(++index, Types.VARCHAR);
        statement.setNull(++index, Types.VARCHAR, "IGNORED");
        statement.setObject(++index, null);
        statement.setObject(++index, null, Types.INTEGER);
        statement.setObject(++index, null, JDBCType.INTEGER);
        statement.setString(++index, null);
        statement.setTime(++index, null);
        statement.setTimestamp(++index, null);
        statement.setURL(++index, null);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), eq(String.join("&",
                queryParam("query", "select * from Fielding where yearid in (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"),
                queryParam("$data_world_param0", "\"not-null\""))));
    }

    @Test
    public void testAllNotSupported() throws Exception {
        PreparedStatement statement = samplePreparedStatement();
        assertSQLFeatureNotSupported(statement::executeUpdate);
        assertSQLFeatureNotSupported(() -> statement.setAsciiStream(1, mock(InputStream.class)));
        assertSQLFeatureNotSupported(() -> statement.setAsciiStream(1, mock(InputStream.class), 100));
        assertSQLFeatureNotSupported(() -> statement.setAsciiStream(1, mock(InputStream.class), 10_000L));
        assertSQLFeatureNotSupported(() -> statement.setBinaryStream(1, mock(InputStream.class)));
        assertSQLFeatureNotSupported(() -> statement.setBinaryStream(1, mock(InputStream.class), 100));
        assertSQLFeatureNotSupported(() -> statement.setBinaryStream(1, mock(InputStream.class), 10_000L));
        assertSQLFeatureNotSupported(() -> statement.setBlob(1, mock(Blob.class)));
        assertSQLFeatureNotSupported(() -> statement.setBlob(1, mock(InputStream.class)));
        assertSQLFeatureNotSupported(() -> statement.setBlob(1, mock(InputStream.class), 100));
        assertSQLFeatureNotSupported(() -> statement.setBytes(1, new byte[100]));
        assertSQLFeatureNotSupported(() -> statement.setCharacterStream(1, mock(Reader.class)));
        assertSQLFeatureNotSupported(() -> statement.setCharacterStream(1, mock(Reader.class), 100));
        assertSQLFeatureNotSupported(() -> statement.setCharacterStream(1, mock(Reader.class), 10_000L));
        assertSQLFeatureNotSupported(() -> statement.setClob(1, mock(Clob.class)));
        assertSQLFeatureNotSupported(() -> statement.setClob(1, mock(Reader.class)));
        assertSQLFeatureNotSupported(() -> statement.setClob(1, mock(Reader.class), 100));
        assertSQLFeatureNotSupported(() -> statement.setDate(1, Date.valueOf(LocalDate.now()), Calendar.getInstance()));
        assertSQLFeatureNotSupported(() -> statement.setNCharacterStream(1, mock(Reader.class)));
        assertSQLFeatureNotSupported(() -> statement.setNCharacterStream(1, mock(Reader.class), 100));
        assertSQLFeatureNotSupported(() -> statement.setNClob(1, mock(NClob.class)));
        assertSQLFeatureNotSupported(() -> statement.setNClob(1, mock(Reader.class)));
        assertSQLFeatureNotSupported(() -> statement.setNClob(1, mock(Reader.class), 100));
        assertSQLFeatureNotSupported(() -> statement.setObject(1, null, Types.DECIMAL, 11));
        assertSQLFeatureNotSupported(() -> statement.setObject(1, 123, mock(SQLType.class)));
        assertSQLFeatureNotSupported(() -> statement.setObject(1, 123, mock(SQLType.class), 11));
        assertSQLFeatureNotSupported(() -> statement.setObject(1, 123, JDBCType.DECIMAL, 11));
        assertSQLFeatureNotSupported(() -> statement.setRowId(1, mock(RowId.class)));
        assertSQLFeatureNotSupported(() -> statement.setSQLXML(1, mock(SQLXML.class)));
        assertSQLFeatureNotSupported(() -> statement.setTime(1, Time.valueOf(LocalTime.now()), Calendar.getInstance()));
        assertSQLFeatureNotSupported(() -> statement.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()), Calendar.getInstance()));
    }

    private static String queryParam(String name, String value) {
        return uriEncode(name) + "=" + uriEncode(value);
    }

    private static String uriEncode(String string) {
        try {
            return URLEncoder.encode(string, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
