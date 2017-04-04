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
package world.data.jdbc.internal.statements;

import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.NanoHTTPD.Method;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import world.data.jdbc.DataWorldCallableStatement;
import world.data.jdbc.DataWorldConnection;
import world.data.jdbc.DataWorldPreparedStatement;
import world.data.jdbc.DataWorldStatement;
import world.data.jdbc.testing.NanoHTTPDHandler;
import world.data.jdbc.testing.NanoHTTPDResource;
import world.data.jdbc.testing.SqlHelper;
import world.data.jdbc.testing.Utils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
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
import java.sql.Types;
import java.time.LocalTime;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;
import static world.data.jdbc.testing.MoreAssertions.assertSQLFeatureNotSupported;

public class PreparedStatementTest {
    private static NanoHTTPDHandler lastBackendRequest;
    private static final String resultResourceName = "/select.json";
    private static final String resultMimeType = Utils.TYPE_SPARQL_RESULTS;

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

    private DataWorldPreparedStatement samplePreparedStatement() throws SQLException {
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
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setBigDecimal(1, new BigDecimal(3));
        statement.execute();
        verify(lastBackendRequest).handle(eq(Method.POST), eq(sql.urlPath()), isNull(), eq(Utils.TYPE_FORM_URLENCODED),
                endsWith(Utils.queryParam("$data_world_param0", "\"3\"^^<http://www.w3.org/2001/XMLSchema#decimal>")));
    }

    @Test
    public void setBoolean() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setBoolean(1, true);
        statement.execute();
        verify(lastBackendRequest).handle(eq(Method.POST), eq(sql.urlPath()), isNull(), eq(Utils.TYPE_FORM_URLENCODED),
                endsWith(Utils.queryParam("$data_world_param0", "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>")));
    }

    @Test
    public void setByte() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setByte(1, (byte) 4);
        statement.execute();
        verify(lastBackendRequest).handle(eq(Method.POST), eq(sql.urlPath()), isNull(), eq(Utils.TYPE_FORM_URLENCODED),
                endsWith(Utils.queryParam("$data_world_param0", "\"4\"^^<http://www.w3.org/2001/XMLSchema#integer>")));
    }

    @Test
    public void setDate() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setDate(1, new Date(1477433443000L));
        statement.execute();
        verify(lastBackendRequest).handle(eq(Method.POST), eq(sql.urlPath()), isNull(), eq(Utils.TYPE_FORM_URLENCODED),
                endsWith(Utils.queryParam("$data_world_param0", "\"2016-10-25\"^^<http://www.w3.org/2001/XMLSchema#date>")));
    }

    @Test
    public void setDouble() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setDouble(1, 3.0);
        statement.execute();
        verify(lastBackendRequest).handle(eq(Method.POST), eq(sql.urlPath()), isNull(), eq(Utils.TYPE_FORM_URLENCODED),
                endsWith(Utils.queryParam("$data_world_param0", "\"3.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>")));
    }

    @Test
    public void setFloat() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setFloat(1, 3.0F);
        statement.execute();
        verify(lastBackendRequest).handle(eq(Method.POST), eq(sql.urlPath()), isNull(), eq(Utils.TYPE_FORM_URLENCODED),
                endsWith(Utils.queryParam("$data_world_param0", "\"3.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>")));
    }

    @Test
    public void setInt() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setInt(1, 3);
        statement.execute();
        verify(lastBackendRequest).handle(eq(Method.POST), eq(sql.urlPath()), isNull(), eq(Utils.TYPE_FORM_URLENCODED),
                endsWith(Utils.queryParam("$data_world_param0", "\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>")));
    }

    @Test
    public void setLong() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setLong(1, 3L);
        statement.execute();
        verify(lastBackendRequest).handle(eq(Method.POST), eq(sql.urlPath()), isNull(), eq(Utils.TYPE_FORM_URLENCODED),
                endsWith(Utils.queryParam("$data_world_param0", "\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>")));
    }

    @Test
    public void setNString() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setNString(1, "foo");
        statement.execute();
        verify(lastBackendRequest).handle(eq(Method.POST), eq(sql.urlPath()), isNull(), eq(Utils.TYPE_FORM_URLENCODED),
                endsWith(Utils.queryParam("$data_world_param0", "\"foo\"")));
    }

    @Test
    public void setNull() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setNull(1, Types.VARCHAR);
        statement.execute();
        verify(lastBackendRequest).handle(Method.POST, sql.urlPath(), null, Utils.TYPE_FORM_URLENCODED,
                Utils.queryParam("query", "select * from Fielding where yearid = ?"));
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
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setShort(1, (short) 4);
        statement.execute();
        verify(lastBackendRequest).handle(eq(Method.POST), eq(sql.urlPath()), isNull(), eq(Utils.TYPE_FORM_URLENCODED),
                endsWith(Utils.queryParam("$data_world_param0", "\"4\"^^<http://www.w3.org/2001/XMLSchema#integer>")));
    }

    @Test
    public void setString() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setString(1, "foo");
        statement.execute();
        verify(lastBackendRequest).handle(eq(Method.POST), eq(sql.urlPath()), isNull(), eq(Utils.TYPE_FORM_URLENCODED),
                endsWith(Utils.queryParam("$data_world_param0", "\"foo\"")));
    }

    @Test
    public void setTime() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setTime(1, Time.valueOf(LocalTime.of(22, 10, 43)));
        statement.execute();
        verify(lastBackendRequest).handle(eq(Method.POST), eq(sql.urlPath()), isNull(), eq(Utils.TYPE_FORM_URLENCODED),
                endsWith(Utils.queryParam("$data_world_param0", "\"22:10:43\"^^<http://www.w3.org/2001/XMLSchema#time>")));
    }


    @Test
    public void testNull() throws Exception {
        DataWorldPreparedStatement statement = sql.prepareStatement(sql.connect(), "select * from Fielding where yearid in (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
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
        verify(lastBackendRequest).handle(Method.POST, sql.urlPath(), null, Utils.TYPE_FORM_URLENCODED, String.join("&",
                Utils.queryParam("query", "select * from Fielding where yearid in (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"),
                Utils.queryParam("$data_world_param0", "\"not-null\"")));
    }

    @Test
    public void testWrapperFor() throws SQLException {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        assertThat(statement.isWrapperFor(DataWorldStatement.class)).isTrue();
        assertThat(statement.isWrapperFor(DataWorldPreparedStatement.class)).isTrue();
        assertThat(statement.isWrapperFor(DataWorldCallableStatement.class)).isFalse();
        assertThat(statement.isWrapperFor(DataWorldConnection.class)).isFalse();
        assertThat(statement.unwrap(DataWorldStatement.class)).isSameAs(statement);
        assertThat(statement.unwrap(DataWorldPreparedStatement.class)).isSameAs(statement);
        assertSQLException(() -> statement.unwrap(DataWorldCallableStatement.class));
    }

    @Test
    public void testAllNotSupported() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
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
    }
}
