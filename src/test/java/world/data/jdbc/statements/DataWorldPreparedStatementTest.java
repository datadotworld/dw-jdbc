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

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static world.data.jdbc.testing.MoreAssertions.assertSQLFeatureNotSupported;

public class DataWorldPreparedStatementTest {
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
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"3\"^^<http://www.w3.org/2001/XMLSchema#decimal>")));
    }

    @Test
    public void setBoolean() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setBoolean(1, true);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>")));
    }

    @Test
    public void setByte() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setByte(1, (byte) 4);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"4\"^^<http://www.w3.org/2001/XMLSchema#byte>")));
    }

    @Test
    public void setDate() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setDate(1, new Date(1477433443000L));
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"2016-10-25T22:10:43Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>")));
    }

    @Test
    public void setDouble() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setDouble(1, 3.0);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"3.0\"^^<http://www.w3.org/2001/XMLSchema#double>")));
    }

    @Test
    public void setFloat() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setFloat(1, 3.0F);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"3.0\"^^<http://www.w3.org/2001/XMLSchema#float>")));
    }

    @Test
    public void setInt() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setInt(1, 3);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>")));
    }

    @Test
    public void setLong() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setLong(1, 3L);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>")));
    }

    @Test
    public void setNString() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setNString(1, "foo");
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"foo\"")));
    }

    @Test
    public void setNull() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
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
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setShort(1, (short) 4);
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"4\"^^<http://www.w3.org/2001/XMLSchema#short>")));
    }

    @Test
    public void setString() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setString(1, "foo");
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"foo\"")));
    }

    @Test
    public void setTime() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        statement.setTime(1, new Time(1477433443000L));
        statement.execute();
        verify(lastBackendRequest).handle(any(), any(), endsWith(
                queryParam("$data_world_param0", "\"22:10:43Z\"^^<http://www.w3.org/2001/XMLSchema#time>")));
    }

    @Test
    public void testAllNotSupported() throws Exception {
        DataWorldPreparedStatement statement = samplePreparedStatement();
        assertSQLFeatureNotSupported(() -> statement.setTimestamp(1, new Timestamp(1477433443000L)));
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
