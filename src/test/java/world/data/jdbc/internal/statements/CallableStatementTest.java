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
import lombok.extern.java.Log;
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
import world.data.jdbc.testing.SparqlHelper;
import world.data.jdbc.testing.SqlHelper;
import world.data.jdbc.testing.Utils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
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
import java.util.Collections;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;
import static world.data.jdbc.testing.MoreAssertions.assertSQLFeatureNotSupported;

@Log
public class CallableStatementTest {
    private static NanoHTTPDHandler lastBackendRequest;

    @ClassRule
    public static final NanoHTTPDResource proxiedServer = new NanoHTTPDResource(3333) {
        @Override
        protected NanoHTTPD.Response serve(NanoHTTPD.IHTTPSession session) throws Exception {
            NanoHTTPDHandler.invoke(session, lastBackendRequest);
            String body = IOUtils.toString(getClass().getResourceAsStream("/select.json"), UTF_8);
            return newResponse(NanoHTTPD.Response.Status.OK, Utils.TYPE_SPARQL_RESULTS, body);
        }
    };

    @Rule
    public final SqlHelper sql = new SqlHelper();

    @Rule
    public final SparqlHelper sparql = new SparqlHelper();

    @Before
    public void setup() {
        lastBackendRequest = mock(NanoHTTPDHandler.class);
    }

    private DataWorldCallableStatement sampleSqlCallableStatement() throws SQLException {
        return sql.prepareCall(sql.connect(), "select * from Fielding where yearid = ?");
    }

    private DataWorldCallableStatement sampleSparqlCallableStatement() throws SQLException {
        return sparql.prepareCall(sparql.connect(), "select ?s ?p ?o where {?s ?p ?o.} limit 10");
    }

    @Test
    public void testNull() throws Exception {
        DataWorldCallableStatement statement = sampleSparqlCallableStatement();
        statement.setString("p", "not-null");
        statement.setBigDecimal("bigdecimal", null);
        statement.setDate("date", null);
        statement.setNString("nstring", null);
        statement.setNull("null", Types.VARCHAR);
        statement.setNull("null2", Types.VARCHAR, "IGNORED");
        statement.setObject("object", null);
        statement.setObject("object", null, Types.INTEGER);
        statement.setObject("object", null, JDBCType.INTEGER);
        statement.setString("string", null);
        statement.setTime("time", null);
        statement.setTimestamp("timestamp", null);
        statement.setURL("url", null);
        statement.execute();
        verify(lastBackendRequest).handle(Method.POST, sparql.urlPath(), null, Utils.TYPE_FORM_URLENCODED, String.join("&",
                Utils.queryParam("query", "select ?s ?p ?o where {?s ?p ?o.} limit 10"),
                Utils.queryParam("$p", "\"not-null\"")));
    }

    @Test
    public void testWrapperFor() throws SQLException {
        DataWorldCallableStatement statement = sampleSparqlCallableStatement();
        assertThat(statement.isWrapperFor(DataWorldStatement.class)).isTrue();
        assertThat(statement.isWrapperFor(DataWorldPreparedStatement.class)).isTrue();
        assertThat(statement.isWrapperFor(DataWorldCallableStatement.class)).isTrue();
        assertThat(statement.isWrapperFor(DataWorldConnection.class)).isFalse();
        assertThat(statement.unwrap(DataWorldStatement.class)).isSameAs(statement);
        assertThat(statement.unwrap(DataWorldPreparedStatement.class)).isSameAs(statement);
        assertThat(statement.unwrap(DataWorldCallableStatement.class)).isSameAs(statement);
        assertSQLException(() -> statement.unwrap(DataWorldConnection.class));
    }

    @Test
    public void testSqlNotSupported() throws Exception {
        // Named parameters aren't supported in sql
        DataWorldCallableStatement statement = sampleSqlCallableStatement();
        assertSQLFeatureNotSupported(() -> statement.setBigDecimal("p", BigDecimal.valueOf(123)));
        assertSQLFeatureNotSupported(() -> statement.setBoolean("p", true));
        assertSQLFeatureNotSupported(() -> statement.setByte("p", (byte) -123));
        assertSQLFeatureNotSupported(() -> statement.setDate("p", Date.valueOf(LocalDate.now())));
        assertSQLFeatureNotSupported(() -> statement.setDouble("p", 123.456789));
        assertSQLFeatureNotSupported(() -> statement.setFloat("p", 123_456.789f));
        assertSQLFeatureNotSupported(() -> statement.setInt("p", 123_456_789));
        assertSQLFeatureNotSupported(() -> statement.setLong("p", 123_456_789_012L));
        assertSQLFeatureNotSupported(() -> statement.setNString("p", "hello world"));
        assertSQLFeatureNotSupported(() -> statement.setNull("p", Types.VARCHAR));
        assertSQLFeatureNotSupported(() -> statement.setNull("p", Types.VARCHAR, "IGNORED"));
        assertSQLFeatureNotSupported(() -> statement.setObject("p", "hello world"));
        assertSQLFeatureNotSupported(() -> statement.setObject("p", 123, Types.INTEGER));
        assertSQLFeatureNotSupported(() -> statement.setObject("p", 123, Types.DECIMAL, 11));
        assertSQLFeatureNotSupported(() -> statement.setShort("p", (short) -12_345));
        assertSQLFeatureNotSupported(() -> statement.setString("p", "hello world"));
        assertSQLFeatureNotSupported(() -> statement.setTime("p", Time.valueOf(LocalTime.now())));
        assertSQLFeatureNotSupported(() -> statement.setTimestamp("p", Timestamp.valueOf(LocalDateTime.now())));
        assertSQLFeatureNotSupported(() -> statement.setURL("p", new URL("http://example.org")));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testAllNotSupported() throws Exception {
        DataWorldCallableStatement sqlStatement = sampleSqlCallableStatement();
        DataWorldCallableStatement sparqlStatement = sampleSparqlCallableStatement();
        for (DataWorldCallableStatement statement : new DataWorldCallableStatement[]{sqlStatement, sparqlStatement}) {
            log.info("testing not-supported for " + (statement == sqlStatement ? "SQL" : "SPARQL"));
            assertSQLFeatureNotSupported(() -> statement.getArray("p"));
            assertSQLFeatureNotSupported(() -> statement.getArray(1));
            assertSQLFeatureNotSupported(() -> statement.getBigDecimal("p"));
            assertSQLFeatureNotSupported(() -> statement.getBigDecimal(1));
            assertSQLFeatureNotSupported(() -> statement.getBigDecimal(1, 11));
            assertSQLFeatureNotSupported(() -> statement.getBlob("p"));
            assertSQLFeatureNotSupported(() -> statement.getBlob(1));
            assertSQLFeatureNotSupported(() -> statement.getBoolean("p"));
            assertSQLFeatureNotSupported(() -> statement.getBoolean(1));
            assertSQLFeatureNotSupported(() -> statement.getByte("p"));
            assertSQLFeatureNotSupported(() -> statement.getByte(1));
            assertSQLFeatureNotSupported(() -> statement.getBytes("p"));
            assertSQLFeatureNotSupported(() -> statement.getBytes(1));
            assertSQLFeatureNotSupported(() -> statement.getCharacterStream("p"));
            assertSQLFeatureNotSupported(() -> statement.getCharacterStream(1));
            assertSQLFeatureNotSupported(() -> statement.getClob("p"));
            assertSQLFeatureNotSupported(() -> statement.getClob(1));
            assertSQLFeatureNotSupported(() -> statement.getDate("p"));
            assertSQLFeatureNotSupported(() -> statement.getDate("p", Calendar.getInstance()));
            assertSQLFeatureNotSupported(() -> statement.getDate(1));
            assertSQLFeatureNotSupported(() -> statement.getDate(1, Calendar.getInstance()));
            assertSQLFeatureNotSupported(() -> statement.getDouble("p"));
            assertSQLFeatureNotSupported(() -> statement.getDouble(1));
            assertSQLFeatureNotSupported(() -> statement.getFloat("p"));
            assertSQLFeatureNotSupported(() -> statement.getFloat(1));
            assertSQLFeatureNotSupported(() -> statement.getInt("p"));
            assertSQLFeatureNotSupported(() -> statement.getInt(1));
            assertSQLFeatureNotSupported(() -> statement.getLong("p"));
            assertSQLFeatureNotSupported(() -> statement.getLong(1));
            assertSQLFeatureNotSupported(() -> statement.getNCharacterStream("p"));
            assertSQLFeatureNotSupported(() -> statement.getNCharacterStream(1));
            assertSQLFeatureNotSupported(() -> statement.getNClob("p"));
            assertSQLFeatureNotSupported(() -> statement.getNClob(1));
            assertSQLFeatureNotSupported(() -> statement.getNString("p"));
            assertSQLFeatureNotSupported(() -> statement.getNString(1));
            assertSQLFeatureNotSupported(() -> statement.getObject("p"));
            assertSQLFeatureNotSupported(() -> statement.getObject("p", Collections.emptyMap()));
            assertSQLFeatureNotSupported(() -> statement.getObject("p", String.class));
            assertSQLFeatureNotSupported(() -> statement.getObject(1));
            assertSQLFeatureNotSupported(() -> statement.getObject(1, Collections.emptyMap()));
            assertSQLFeatureNotSupported(() -> statement.getObject(1, String.class));
            assertSQLFeatureNotSupported(() -> statement.getRef("p"));
            assertSQLFeatureNotSupported(() -> statement.getRef(1));
            assertSQLFeatureNotSupported(() -> statement.getRowId("p"));
            assertSQLFeatureNotSupported(() -> statement.getRowId(1));
            assertSQLFeatureNotSupported(() -> statement.getSQLXML("p"));
            assertSQLFeatureNotSupported(() -> statement.getSQLXML(1));
            assertSQLFeatureNotSupported(() -> statement.getShort("p"));
            assertSQLFeatureNotSupported(() -> statement.getShort(1));
            assertSQLFeatureNotSupported(() -> statement.getString("p"));
            assertSQLFeatureNotSupported(() -> statement.getString(1));
            assertSQLFeatureNotSupported(() -> statement.getTime("p"));
            assertSQLFeatureNotSupported(() -> statement.getTime("p", Calendar.getInstance()));
            assertSQLFeatureNotSupported(() -> statement.getTime(1));
            assertSQLFeatureNotSupported(() -> statement.getTime(1, Calendar.getInstance()));
            assertSQLFeatureNotSupported(() -> statement.getTimestamp("p"));
            assertSQLFeatureNotSupported(() -> statement.getTimestamp("p", Calendar.getInstance()));
            assertSQLFeatureNotSupported(() -> statement.getTimestamp(1));
            assertSQLFeatureNotSupported(() -> statement.getTimestamp(1, Calendar.getInstance()));
            assertSQLFeatureNotSupported(() -> statement.getURL("p"));
            assertSQLFeatureNotSupported(() -> statement.getURL(1));
            assertSQLFeatureNotSupported(() -> statement.registerOutParameter("p", Types.VARCHAR));
            assertSQLFeatureNotSupported(() -> statement.registerOutParameter(1, Types.VARCHAR));
            assertSQLFeatureNotSupported(() -> statement.registerOutParameter("p", Types.VARCHAR, "IGNORED"));
            assertSQLFeatureNotSupported(() -> statement.registerOutParameter(1, Types.VARCHAR, "IGNORED"));
            assertSQLFeatureNotSupported(() -> statement.registerOutParameter("p", Types.DECIMAL, 11));
            assertSQLFeatureNotSupported(() -> statement.registerOutParameter(1, Types.DECIMAL, 11));
            assertSQLFeatureNotSupported(() -> statement.setAsciiStream("p", mock(InputStream.class)));
            assertSQLFeatureNotSupported(() -> statement.setAsciiStream("p", mock(InputStream.class), 100));
            assertSQLFeatureNotSupported(() -> statement.setAsciiStream("p", mock(InputStream.class), 10_000L));
            assertSQLFeatureNotSupported(() -> statement.setBinaryStream("p", mock(InputStream.class)));
            assertSQLFeatureNotSupported(() -> statement.setBinaryStream("p", mock(InputStream.class), 100));
            assertSQLFeatureNotSupported(() -> statement.setBinaryStream("p", mock(InputStream.class), 10_000L));
            assertSQLFeatureNotSupported(() -> statement.setBlob("p", mock(Blob.class)));
            assertSQLFeatureNotSupported(() -> statement.setBlob("p", mock(InputStream.class)));
            assertSQLFeatureNotSupported(() -> statement.setBlob("p", mock(InputStream.class), 100));
            assertSQLFeatureNotSupported(() -> statement.setBytes("p", new byte[100]));
            assertSQLFeatureNotSupported(() -> statement.setCharacterStream("p", mock(Reader.class)));
            assertSQLFeatureNotSupported(() -> statement.setCharacterStream("p", mock(Reader.class), 100));
            assertSQLFeatureNotSupported(() -> statement.setCharacterStream("p", mock(Reader.class), 10_000L));
            assertSQLFeatureNotSupported(() -> statement.setClob("p", mock(Clob.class)));
            assertSQLFeatureNotSupported(() -> statement.setClob("p", mock(Reader.class)));
            assertSQLFeatureNotSupported(() -> statement.setClob("p", mock(Reader.class), 100));
            assertSQLFeatureNotSupported(() -> statement.setDate("p", Date.valueOf(LocalDate.now()), Calendar.getInstance()));
            assertSQLFeatureNotSupported(() -> statement.setNCharacterStream("p", mock(Reader.class)));
            assertSQLFeatureNotSupported(() -> statement.setNCharacterStream("p", mock(Reader.class), 100));
            assertSQLFeatureNotSupported(() -> statement.setNClob("p", mock(NClob.class)));
            assertSQLFeatureNotSupported(() -> statement.setNClob("p", mock(Reader.class)));
            assertSQLFeatureNotSupported(() -> statement.setNClob("p", mock(Reader.class), 100));
            assertSQLFeatureNotSupported(() -> statement.setObject("p", null, Types.DECIMAL, 11));
            assertSQLFeatureNotSupported(() -> statement.setObject("p", 123, mock(SQLType.class)));
            assertSQLFeatureNotSupported(() -> statement.setObject("p", 123, mock(SQLType.class), 11));
            assertSQLFeatureNotSupported(() -> statement.setObject("p", 123, JDBCType.DECIMAL, 11));
            assertSQLFeatureNotSupported(() -> statement.setRowId("p", mock(RowId.class)));
            assertSQLFeatureNotSupported(() -> statement.setSQLXML("p", mock(SQLXML.class)));
            assertSQLFeatureNotSupported(() -> statement.setTime("p", Time.valueOf(LocalTime.now()), Calendar.getInstance()));
            assertSQLFeatureNotSupported(() -> statement.setTimestamp("p", Timestamp.valueOf(LocalDateTime.now()), Calendar.getInstance()));
            assertSQLFeatureNotSupported(statement::wasNull);
        }
    }
}
