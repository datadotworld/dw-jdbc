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
package world.data.jdbc.internal.results;

import fi.iki.elonen.NanoHTTPD;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import world.data.jdbc.DataWorldStatement;
import world.data.jdbc.model.Blank;
import world.data.jdbc.model.Iri;
import world.data.jdbc.model.Literal;
import world.data.jdbc.model.LiteralFactory;
import world.data.jdbc.model.Node;
import world.data.jdbc.testing.NanoHTTPDHandler;
import world.data.jdbc.testing.NanoHTTPDResource;
import world.data.jdbc.testing.SparqlHelper;
import world.data.jdbc.testing.Utils;
import world.data.jdbc.vocab.Xsd;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Year;
import java.util.Collections;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;
import static world.data.jdbc.testing.MoreAssertions.assertSQLFeatureNotSupported;

public class ResultSetTest {
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
    public final SparqlHelper sparql = new SparqlHelper();

    @Before
    public void setup() {
        lastBackendRequest = mock(NanoHTTPDHandler.class);
    }

    private ResultSet sampleResultSet() throws SQLException {
        DataWorldStatement statement = sparql.createStatement(sparql.connect());
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
        ResultSet resultSet = sampleResultSet();

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getObject("s")).isEqualTo("http://data.world/user8/lahman-baseball/");
        assertThat(resultSet.getObject("p")).isEqualTo("http://data.world#agentId");
        assertThat(resultSet.getObject("o")).isEqualTo("user8");
        assertThat(resultSet.getObject(1)).isEqualTo("http://data.world/user8/lahman-baseball/");
        assertThat(resultSet.getObject(3)).isEqualTo("user8");

        assertThat(resultSet.next()).isTrue();

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getObject(3)).isEqualTo("1475533105077");
    }

    @Test
    public void getObject1() throws Exception {
        ResultSet resultSet = sampleResultSet();

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getObject("s", Node.class)).isEqualTo(new Iri("http://data.world/user8/lahman-baseball/"));
        assertThat(resultSet.getObject("p", Node.class)).isEqualTo(new Iri("http://data.world#agentId"));
        assertThat(resultSet.getObject("o", Node.class)).isEqualTo(LiteralFactory.createString("user8"));
        assertThat(resultSet.getObject(1, Node.class)).isEqualTo(new Iri("http://data.world/user8/lahman-baseball/"));
        assertThat(resultSet.getObject(1, Iri.class)).isEqualTo(new Iri("http://data.world/user8/lahman-baseball/"));
        assertThat(resultSet.getObject(1, URI.class)).isEqualTo(URI.create("http://data.world/user8/lahman-baseball/"));
        assertThat(resultSet.getObject(1, URL.class)).isEqualTo(new URL("http://data.world/user8/lahman-baseball/"));
        assertThat(resultSet.getObject(1, String.class)).isEqualTo("http://data.world/user8/lahman-baseball/");
        assertSQLException(() -> resultSet.getObject(1, Blank.class));
        assertSQLException(() -> resultSet.getObject(1, Literal.class));
        assertSQLException(() -> resultSet.getObject(1, (Class<?>) null));
        assertThat(resultSet.getObject(3, Node.class)).isEqualTo(LiteralFactory.createString("user8"));
        assertThat(resultSet.getObject(3, Literal.class)).isEqualTo(LiteralFactory.createString("user8"));
        assertThat(resultSet.getObject(3, String.class)).isEqualTo("user8");
        assertThat(resultSet.getObject(3, Boolean.class)).isFalse();
        assertSQLException(() -> resultSet.getObject(3, Blank.class));
        assertSQLException(() -> resultSet.getObject(3, Iri.class));
        assertSQLException(() -> resultSet.getObject(3, Number.class));

        // Move to a row that has an xsd:long in the third column
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getObject(3, Node.class)).isEqualTo(new Literal("1475533105077", Xsd.LONG));
        assertThat(resultSet.getObject(3, Literal.class)).isEqualTo(new Literal("1475533105077", Xsd.LONG));
        assertThat(resultSet.getObject(3, String.class)).isEqualTo("1475533105077");
        assertThat(resultSet.getObject(3, Long.class)).isEqualTo(1475533105077L);
        assertThat(resultSet.getObject(3, Number.class)).isEqualTo(1475533105077L);
        assertThat(resultSet.getObject(3, Float.class)).isEqualTo(1475533105077f);
        assertThat(resultSet.getObject(3, Double.class)).isEqualTo(1475533105077d);
        assertThat(resultSet.getObject(3, BigInteger.class)).isEqualTo(new BigInteger("1475533105077"));
        assertThat(resultSet.getObject(3, BigDecimal.class)).isEqualTo(new BigDecimal("1475533105077"));
        assertThat(resultSet.getObject(3, Boolean.class)).isEqualTo(true);
        assertSQLException(() -> resultSet.getObject(3, Blank.class));
        assertSQLException(() -> resultSet.getObject(3, Iri.class));
        assertSQLException(() -> resultSet.getObject(3, Integer.class));
        assertSQLException(() -> resultSet.getObject(3, Short.class));
        assertSQLException(() -> resultSet.getObject(3, Byte.class));
        assertSQLException(() -> resultSet.getObject(3, Year.class));

        // Move to a row that has an xsd:boolean in the third column
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getObject(3, Node.class)).isEqualTo(LiteralFactory.FALSE);
        assertThat(resultSet.getObject(3, Literal.class)).isEqualTo(LiteralFactory.FALSE);
        assertThat(resultSet.getObject(3, String.class)).isEqualTo("false");
        assertThat(resultSet.getObject(3, Boolean.class)).isEqualTo(false);
        assertThat(resultSet.getObject(3, Byte.class)).isEqualTo((byte) 0);
        assertThat(resultSet.getObject(3, Short.class)).isEqualTo((short) 0);
        assertThat(resultSet.getObject(3, Integer.class)).isEqualTo(0);
        assertThat(resultSet.getObject(3, Long.class)).isEqualTo(0L);
        assertThat(resultSet.getObject(3, Number.class)).isEqualTo(0L);
        assertThat(resultSet.getObject(3, Float.class)).isEqualTo(0f);
        assertThat(resultSet.getObject(3, Double.class)).isEqualTo(0d);
        assertThat(resultSet.getObject(3, BigInteger.class)).isEqualTo(BigInteger.ZERO);
        assertThat(resultSet.getObject(3, BigDecimal.class)).isEqualTo(BigDecimal.ZERO);
        assertSQLException(() -> resultSet.getObject(3, Blank.class));
        assertSQLException(() -> resultSet.getObject(3, Iri.class));
        assertSQLException(() -> resultSet.getObject(3, Year.class));
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
    public void wasNull() throws Exception {

    }

    @Test
    public void testForwardOnly() throws Exception {
        ResultSet resultSet = sampleResultSet();

        assertThat(resultSet.getType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
        assertThat(resultSet.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
        resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);  // no-op

        assertSQLException(() -> resultSet.absolute(1));
        assertSQLException(resultSet::afterLast);
        assertSQLException(resultSet::beforeFirst);
        assertSQLException(resultSet::first);
        assertSQLException(resultSet::getRow);
        assertSQLException(resultSet::isAfterLast);
        assertSQLException(resultSet::isBeforeFirst);
        assertSQLException(resultSet::isFirst);
        assertSQLException(resultSet::isLast);
        assertSQLException(resultSet::last);
        assertSQLException(resultSet::previous);
        assertSQLException(resultSet::refreshRow);
        assertSQLException(() -> resultSet.relative(1));
        assertSQLException(() -> resultSet.setFetchDirection(ResultSet.FETCH_REVERSE));
    }

    @Test
    public void testReadOnly() throws Exception {
        ResultSet resultSet = sampleResultSet();

        assertThat(resultSet.getConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
        assertThat(resultSet.rowDeleted()).isFalse();
        assertThat(resultSet.rowInserted()).isFalse();
        assertThat(resultSet.rowUpdated()).isFalse();

        assertSQLException(resultSet::cancelRowUpdates);
        assertSQLException(resultSet::deleteRow);
        assertSQLException(resultSet::insertRow);
        assertSQLException(resultSet::moveToCurrentRow);
        assertSQLException(resultSet::moveToInsertRow);
        assertSQLException(() -> resultSet.updateArray("s", null));
        assertSQLException(() -> resultSet.updateArray(3, null));
        assertSQLException(() -> resultSet.updateAsciiStream("s", null));
        assertSQLException(() -> resultSet.updateAsciiStream("s", null, 3));
        assertSQLException(() -> resultSet.updateAsciiStream("s", null, 3L));
        assertSQLException(() -> resultSet.updateAsciiStream(3, null));
        assertSQLException(() -> resultSet.updateAsciiStream(3, null, 3));
        assertSQLException(() -> resultSet.updateAsciiStream(3, null, 3L));
        assertSQLException(() -> resultSet.updateBigDecimal("s", null));
        assertSQLException(() -> resultSet.updateBigDecimal(3, null));
        assertSQLException(() -> resultSet.updateBinaryStream("s", null));
        assertSQLException(() -> resultSet.updateBinaryStream("s", null, 3));
        assertSQLException(() -> resultSet.updateBinaryStream("s", null, 3L));
        assertSQLException(() -> resultSet.updateBinaryStream(3, null));
        assertSQLException(() -> resultSet.updateBinaryStream(3, null, 3));
        assertSQLException(() -> resultSet.updateBinaryStream(3, null, 3L));
        assertSQLException(() -> resultSet.updateBlob("s", (Blob) null));
        assertSQLException(() -> resultSet.updateBlob("s", (InputStream) null));
        assertSQLException(() -> resultSet.updateBlob("s", null, 3));
        assertSQLException(() -> resultSet.updateBlob(3, (Blob) null));
        assertSQLException(() -> resultSet.updateBlob(3, (InputStream) null));
        assertSQLException(() -> resultSet.updateBlob(3, null, 3L));
        assertSQLException(() -> resultSet.updateBoolean("s", true));
        assertSQLException(() -> resultSet.updateBoolean(3, true));
        assertSQLException(() -> resultSet.updateByte("s", (byte) 2));
        assertSQLException(() -> resultSet.updateByte(3, (byte) 2));
        assertSQLException(() -> resultSet.updateBytes("s", null));
        assertSQLException(() -> resultSet.updateBytes(3, null));
        assertSQLException(() -> resultSet.updateCharacterStream("s", null));
        assertSQLException(() -> resultSet.updateCharacterStream("s", null, 3));
        assertSQLException(() -> resultSet.updateCharacterStream("s", null, 3L));
        assertSQLException(() -> resultSet.updateCharacterStream(3, null));
        assertSQLException(() -> resultSet.updateCharacterStream(3, null, 3));
        assertSQLException(() -> resultSet.updateCharacterStream(3, null, 3L));
        assertSQLException(() -> resultSet.updateClob("s", (Clob) null));
        assertSQLException(() -> resultSet.updateClob("s", (Reader) null));
        assertSQLException(() -> resultSet.updateClob("s", null, 3));
        assertSQLException(() -> resultSet.updateClob(3, (Clob) null));
        assertSQLException(() -> resultSet.updateClob(3, (Reader) null));
        assertSQLException(() -> resultSet.updateClob(3, null, 3L));
        assertSQLException(() -> resultSet.updateDate("s", null));
        assertSQLException(() -> resultSet.updateDate(3, null));
        assertSQLException(() -> resultSet.updateDouble("s", 3.0));
        assertSQLException(() -> resultSet.updateDouble(3, 3.0));
        assertSQLException(() -> resultSet.updateFloat("s", 3.0f));
        assertSQLException(() -> resultSet.updateFloat(3, 3.0f));
        assertSQLException(() -> resultSet.updateInt("s", 3));
        assertSQLException(() -> resultSet.updateInt(3, 4));
        assertSQLException(() -> resultSet.updateLong("s", 3L));
        assertSQLException(() -> resultSet.updateLong(3, 3L));
        assertSQLException(() -> resultSet.updateNCharacterStream("s", null));
        assertSQLException(() -> resultSet.updateNCharacterStream("s", null, 3));
        assertSQLException(() -> resultSet.updateNCharacterStream("s", null, 3L));
        assertSQLException(() -> resultSet.updateNCharacterStream(3, null));
        assertSQLException(() -> resultSet.updateNCharacterStream(3, null, 3));
        assertSQLException(() -> resultSet.updateNCharacterStream(3, null, 3L));
        assertSQLException(() -> resultSet.updateNClob("s", (NClob) null));
        assertSQLException(() -> resultSet.updateNClob("s", (Reader) null));
        assertSQLException(() -> resultSet.updateNClob("s", null, 3));
        assertSQLException(() -> resultSet.updateNClob(3, (NClob) null));
        assertSQLException(() -> resultSet.updateNClob(3, (Reader) null));
        assertSQLException(() -> resultSet.updateNClob(3, null, 3L));
        assertSQLException(() -> resultSet.updateNString("bar", "s"));
        assertSQLException(() -> resultSet.updateNString(3, "s"));
        assertSQLException(() -> resultSet.updateNull("bar"));
        assertSQLException(() -> resultSet.updateNull(3));
        assertSQLException(() -> resultSet.updateObject("bar", null));
        assertSQLException(() -> resultSet.updateObject("bar", null, 2));
        assertSQLException(() -> resultSet.updateObject(3, null));
        assertSQLException(() -> resultSet.updateObject(3, null, 2));
        assertSQLException(() -> resultSet.updateRef("s", null));
        assertSQLException(() -> resultSet.updateRef(3, null));
        assertSQLException(resultSet::updateRow);
        assertSQLException(() -> resultSet.updateRowId("s", null));
        assertSQLException(() -> resultSet.updateRowId(3, null));
        assertSQLException(() -> resultSet.updateSQLXML("s", null));
        assertSQLException(() -> resultSet.updateSQLXML(3, null));
        assertSQLException(() -> resultSet.updateShort("s", (short) 2));
        assertSQLException(() -> resultSet.updateShort(3, (short) 2));
        assertSQLException(() -> resultSet.updateString("s", "bar"));
        assertSQLException(() -> resultSet.updateString(3, "bar"));
        assertSQLException(() -> resultSet.updateTime("s", null));
        assertSQLException(() -> resultSet.updateTime(3, null));
        assertSQLException(() -> resultSet.updateTimestamp("s", null));
        assertSQLException(() -> resultSet.updateTimestamp(3, null));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testAllNotSupported() throws Exception {
        ResultSet resultSet = sampleResultSet();

        assertSQLFeatureNotSupported(() -> resultSet.getArray("s"));
        assertSQLFeatureNotSupported(() -> resultSet.getArray(1));
        assertSQLFeatureNotSupported(() -> resultSet.getAsciiStream("s"));
        assertSQLFeatureNotSupported(() -> resultSet.getAsciiStream(1));
        assertSQLFeatureNotSupported(() -> resultSet.getBigDecimal("s", 3));
        assertSQLFeatureNotSupported(() -> resultSet.getBigDecimal(3, 3));
        assertSQLFeatureNotSupported(() -> resultSet.getBinaryStream("s"));
        assertSQLFeatureNotSupported(() -> resultSet.getBinaryStream(3));
        assertSQLFeatureNotSupported(() -> resultSet.getBlob("s"));
        assertSQLFeatureNotSupported(() -> resultSet.getBlob(3));
        assertSQLFeatureNotSupported(() -> resultSet.getBytes("s"));
        assertSQLFeatureNotSupported(() -> resultSet.getBytes(3));
        assertSQLFeatureNotSupported(() -> resultSet.getCharacterStream("s"));
        assertSQLFeatureNotSupported(() -> resultSet.getCharacterStream(3));
        assertSQLFeatureNotSupported(() -> resultSet.getClob("s"));
        assertSQLFeatureNotSupported(() -> resultSet.getClob(3));
        assertSQLFeatureNotSupported(resultSet::getCursorName);
        assertSQLFeatureNotSupported(() -> resultSet.getDate("s", null));
        assertSQLFeatureNotSupported(() -> resultSet.getDate(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.getNCharacterStream("s"));
        assertSQLFeatureNotSupported(() -> resultSet.getNCharacterStream(3));
        assertSQLFeatureNotSupported(() -> resultSet.getNClob("s"));
        assertSQLFeatureNotSupported(() -> resultSet.getNClob(3));
        assertSQLFeatureNotSupported(() -> resultSet.getObject("s", Collections.emptyMap()));
        assertSQLFeatureNotSupported(() -> resultSet.getObject(3, Collections.emptyMap()));
        assertSQLFeatureNotSupported(() -> resultSet.getRef("s"));
        assertSQLFeatureNotSupported(() -> resultSet.getRef(3));
        assertSQLFeatureNotSupported(() -> resultSet.getRowId("s"));
        assertSQLFeatureNotSupported(() -> resultSet.getRowId(3));
        assertSQLFeatureNotSupported(() -> resultSet.getSQLXML("s"));
        assertSQLFeatureNotSupported(() -> resultSet.getSQLXML(3));
        assertSQLFeatureNotSupported(() -> resultSet.getTime("s", null));
        assertSQLFeatureNotSupported(() -> resultSet.getTime(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.getTimestamp("s", null));
        assertSQLFeatureNotSupported(() -> resultSet.getTimestamp(3, null));
        assertSQLFeatureNotSupported(() -> resultSet.getUnicodeStream("s"));
        assertSQLFeatureNotSupported(() -> resultSet.getUnicodeStream(3));
        assertSQLFeatureNotSupported(() -> resultSet.isWrapperFor(Class.class));
        assertSQLFeatureNotSupported(() -> resultSet.unwrap(Class.class));
    }
}
