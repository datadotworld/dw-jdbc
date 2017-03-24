/*
* dw-jdbc
* Copyright 2016 data.world, Inc.

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
package world.data.jdbc.results;

import fi.iki.elonen.NanoHTTPD;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import world.data.jdbc.statements.Statement;
import world.data.jdbc.testing.NanoHTTPDHandler;
import world.data.jdbc.testing.NanoHTTPDResource;
import world.data.jdbc.testing.SparqlHelper;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static world.data.jdbc.testing.MoreAssertions.assertSQLFeatureNotSupported;

public class ResultsSetTest {
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
    public final SparqlHelper sparql = new SparqlHelper();

    @Before
    public void setup() {
        lastBackendRequest = mock(NanoHTTPDHandler.class);
    }

    private ResultSet sampleResultSet() throws SQLException {
        Statement statement = sparql.createStatement(sparql.connect());
        return sparql.executeQuery(statement, "select ?s ?p ?o where {?s ?p ?o.}");
    }

    @Test
    public void getHoldability() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.getHoldability()).isEqualTo(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Test
    public void getConcurrency() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.getConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
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

    @Test
    public void getStatement() throws Exception {

    }

    @Test
    public void getWarnings() throws Exception {

    }

    @Test
    public void refreshRow() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.refreshRow();
    }

    @Test
    public void rowDeleted() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.rowDeleted()).isFalse();
    }

    @Test
    public void rowInserted() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.rowInserted()).isFalse();
    }

    @Test
    public void rowUpdated() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.rowUpdated()).isFalse();
    }

    @Test
    public void wasNull() throws Exception {

    }

    @SuppressWarnings("deprecation")
    @Test
    public void testAllNotSupported() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertSQLFeatureNotSupported(resultSet::cancelRowUpdates);
        assertSQLFeatureNotSupported(resultSet::deleteRow);
        assertSQLFeatureNotSupported(() -> resultSet.getArray("foo"));
        assertSQLFeatureNotSupported(() -> resultSet.getArray(1));
        assertSQLFeatureNotSupported(() -> resultSet.getAsciiStream("foo"));
        assertSQLFeatureNotSupported(() -> resultSet.getAsciiStream(1));
        assertSQLFeatureNotSupported(() -> resultSet.getBigDecimal("foo", 3));
        assertSQLFeatureNotSupported(() -> resultSet.getBigDecimal(3, 3));
        assertSQLFeatureNotSupported(() -> resultSet.getBinaryStream("foo"));
        assertSQLFeatureNotSupported(() -> resultSet.getBinaryStream(3));
        assertSQLFeatureNotSupported(() -> resultSet.getBlob("foo"));
        assertSQLFeatureNotSupported(() -> resultSet.getBlob(3));
        assertSQLFeatureNotSupported(() -> resultSet.getBytes("foo"));
        assertSQLFeatureNotSupported(() -> resultSet.getBytes(3));
        assertSQLFeatureNotSupported(() -> resultSet.getCharacterStream("foo"));
        assertSQLFeatureNotSupported(() -> resultSet.getCharacterStream(3));
        assertSQLFeatureNotSupported(() -> resultSet.getClob("foo"));
        assertSQLFeatureNotSupported(() -> resultSet.getClob(3));
        assertSQLFeatureNotSupported(resultSet::getCursorName);
        assertSQLFeatureNotSupported(() -> resultSet.getDate("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.getDate(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.getNCharacterStream("foo"));
        assertSQLFeatureNotSupported(() -> resultSet.getNCharacterStream(3));
        assertSQLFeatureNotSupported(() -> resultSet.getNClob("foo"));
        assertSQLFeatureNotSupported(() -> resultSet.getNClob(3));
        assertSQLFeatureNotSupported(() -> resultSet.getObject("foo", Collections.emptyMap()));
        assertSQLFeatureNotSupported(() -> resultSet.getObject("foo", String.class));
        assertSQLFeatureNotSupported(() -> resultSet.getObject(3, Collections.emptyMap()));
        assertSQLFeatureNotSupported(() -> resultSet.getObject(3, String.class));
        assertSQLFeatureNotSupported(() -> resultSet.getRef("foo"));
        assertSQLFeatureNotSupported(() -> resultSet.getRef(3));
        assertSQLFeatureNotSupported(() -> resultSet.getRowId("foo"));
        assertSQLFeatureNotSupported(() -> resultSet.getRowId(3));
        assertSQLFeatureNotSupported(() -> resultSet.getSQLXML("foo"));
        assertSQLFeatureNotSupported(() -> resultSet.getSQLXML(3));
        assertSQLFeatureNotSupported(() -> resultSet.getTime("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.getTime(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.getTimestamp("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.getTimestamp(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.getUnicodeStream("foo"));
        assertSQLFeatureNotSupported(() -> resultSet.getUnicodeStream(3));
        assertSQLFeatureNotSupported(resultSet::insertRow);
        assertSQLFeatureNotSupported(() -> resultSet.isWrapperFor(Class.class));
        assertSQLFeatureNotSupported(resultSet::moveToCurrentRow);
        assertSQLFeatureNotSupported(resultSet::moveToInsertRow);
        assertSQLFeatureNotSupported(resultSet::previous);
        assertSQLFeatureNotSupported(() -> resultSet.unwrap(Class.class));
        assertSQLFeatureNotSupported(() -> resultSet.updateArray("s", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateArray(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.updateAsciiStream("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateAsciiStream("foo", null, 3));
        assertSQLFeatureNotSupported(() -> resultSet.updateAsciiStream("foo", null, 3L));
        assertSQLFeatureNotSupported(() -> resultSet.updateAsciiStream(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.updateAsciiStream(3, null, 3));
        assertSQLFeatureNotSupported(() -> resultSet.updateAsciiStream(3, null, 3L));
        assertSQLFeatureNotSupported(() -> resultSet.updateBigDecimal("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateBigDecimal(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.updateBinaryStream("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateBinaryStream("foo", null, 3));
        assertSQLFeatureNotSupported(() -> resultSet.updateBinaryStream("foo", null, 3L));
        assertSQLFeatureNotSupported(() -> resultSet.updateBinaryStream(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.updateBinaryStream(3, null, 3));
        assertSQLFeatureNotSupported(() -> resultSet.updateBinaryStream(3, null, 3L));
        assertSQLFeatureNotSupported(() -> resultSet.updateBlob("foo", (Blob) null));
        assertSQLFeatureNotSupported(() -> resultSet.updateBlob("foo", (InputStream) null));
        assertSQLFeatureNotSupported(() -> resultSet.updateBlob("foo", null, 3));
        assertSQLFeatureNotSupported(() -> resultSet.updateBlob(3, (Blob) null));
        assertSQLFeatureNotSupported(() -> resultSet.updateBlob(3, (InputStream) null));
        assertSQLFeatureNotSupported(() -> resultSet.updateBlob(3, null, 3L));
        assertSQLFeatureNotSupported(() -> resultSet.updateBoolean("foo", true));
        assertSQLFeatureNotSupported(() -> resultSet.updateBoolean(3, true));
        assertSQLFeatureNotSupported(() -> resultSet.updateByte("foo", (byte) 2));
        assertSQLFeatureNotSupported(() -> resultSet.updateByte(3, (byte) 2));
        assertSQLFeatureNotSupported(() -> resultSet.updateBytes("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateBytes(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.updateCharacterStream("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateCharacterStream("foo", null, 3));
        assertSQLFeatureNotSupported(() -> resultSet.updateCharacterStream("foo", null, 3L));
        assertSQLFeatureNotSupported(() -> resultSet.updateCharacterStream(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.updateCharacterStream(3, null, 3));
        assertSQLFeatureNotSupported(() -> resultSet.updateCharacterStream(3, null, 3L));
        assertSQLFeatureNotSupported(() -> resultSet.updateClob("foo", (Clob) null));
        assertSQLFeatureNotSupported(() -> resultSet.updateClob("foo", (Reader) null));
        assertSQLFeatureNotSupported(() -> resultSet.updateClob("foo", null, 3));
        assertSQLFeatureNotSupported(() -> resultSet.updateClob(3, (Clob) null));
        assertSQLFeatureNotSupported(() -> resultSet.updateClob(3, (Reader) null));
        assertSQLFeatureNotSupported(() -> resultSet.updateClob(3, null, 3L));
        assertSQLFeatureNotSupported(() -> resultSet.updateDate("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateDate(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.updateDouble("foo", 3.0));
        assertSQLFeatureNotSupported(() -> resultSet.updateDouble(3, 3.0));
        assertSQLFeatureNotSupported(() -> resultSet.updateFloat("foo", 3.0f));
        assertSQLFeatureNotSupported(() -> resultSet.updateFloat(3, 3.0f));
        assertSQLFeatureNotSupported(() -> resultSet.updateInt("foo", 3));
        assertSQLFeatureNotSupported(() -> resultSet.updateInt(3, 4));
        assertSQLFeatureNotSupported(() -> resultSet.updateLong("foo", 3L));
        assertSQLFeatureNotSupported(() -> resultSet.updateLong(3, 3L));
        assertSQLFeatureNotSupported(() -> resultSet.updateNCharacterStream("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateNCharacterStream("foo", null, 3));
        assertSQLFeatureNotSupported(() -> resultSet.updateNCharacterStream("foo", null, 3L));
        assertSQLFeatureNotSupported(() -> resultSet.updateNCharacterStream(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.updateNCharacterStream(3, null, 3));
        assertSQLFeatureNotSupported(() -> resultSet.updateNCharacterStream(3, null, 3L));
        assertSQLFeatureNotSupported(() -> resultSet.updateNClob("foo", (NClob) null));
        assertSQLFeatureNotSupported(() -> resultSet.updateNClob("foo", (Reader) null));
        assertSQLFeatureNotSupported(() -> resultSet.updateNClob("foo", null, 3));
        assertSQLFeatureNotSupported(() -> resultSet.updateNClob(3, (NClob) null));
        assertSQLFeatureNotSupported(() -> resultSet.updateNClob(3, (Reader) null));
        assertSQLFeatureNotSupported(() -> resultSet.updateNClob(3, null, 3L));
        assertSQLFeatureNotSupported(() -> resultSet.updateNString("bar", "foo"));
        assertSQLFeatureNotSupported(() -> resultSet.updateNString(3, "foo"));
        assertSQLFeatureNotSupported(() -> resultSet.updateNull("bar"));
        assertSQLFeatureNotSupported(() -> resultSet.updateNull(3));
        assertSQLFeatureNotSupported(() -> resultSet.updateObject("bar", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateObject("bar", null, 2));
        assertSQLFeatureNotSupported(() -> resultSet.updateObject(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.updateObject(3, null, 2));
        assertSQLFeatureNotSupported(() -> resultSet.updateRef("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateRef(3, null));
        assertSQLFeatureNotSupported(resultSet::updateRow);
        assertSQLFeatureNotSupported(() -> resultSet.updateRowId("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateRowId(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.updateSQLXML("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateSQLXML(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.updateShort("foo", (short) 2));
        assertSQLFeatureNotSupported(() -> resultSet.updateShort(3, (short) 2));
        assertSQLFeatureNotSupported(() -> resultSet.updateString("foo", "bar"));
        assertSQLFeatureNotSupported(() -> resultSet.updateString(3, "bar"));
        assertSQLFeatureNotSupported(() -> resultSet.updateTime("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateTime(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.updateTimestamp("foo", null));
        assertSQLFeatureNotSupported(() -> resultSet.updateTimestamp(3, null));
    }
}
