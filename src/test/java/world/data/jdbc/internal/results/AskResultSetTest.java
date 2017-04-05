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
import fi.iki.elonen.NanoHTTPD.Method;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import world.data.jdbc.DataWorldStatement;
import world.data.jdbc.testing.NanoHTTPDHandler;
import world.data.jdbc.testing.NanoHTTPDResource;
import world.data.jdbc.testing.SparqlHelper;
import world.data.jdbc.testing.Utils;

import java.sql.ResultSet;
import java.sql.SQLException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;

public class AskResultSetTest {

    private static NanoHTTPDHandler lastBackendRequest;
    private static final String resultResourceName = "/ask.json";
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
        return sparql.executeQuery(statement, "ask{?s ?p ?o.}");
    }

    @Test
    public void findColumn() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.findColumn("ASK")).isEqualTo(1);
    }

    @Test
    public void findColumnFail() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertSQLException(() -> resultSet.findColumn("BAD_ASK"));
    }

    @Test
    public void getFetchSize() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.getFetchSize()).isEqualTo(0);
    }

    @Test
    public void isClosed() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.isClosed()).isFalse();
        resultSet.close();
        assertThat(resultSet.isClosed()).isTrue();
    }

    @Test
    public void findColumnLabel() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.getMetaData().getColumnLabel(1)).isEqualTo("ASK");
        assertSQLException(() -> resultSet.getMetaData().getColumnLabel(0));
    }

    @Test
    public void getNode() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertSQLException(() -> resultSet.getString("FOO"));
    }

    @Test
    public void getBoolean() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getBoolean("ASK")).isTrue();
    }

    @Test
    public void getBooleanBadRow() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.next()).isFalse();
        assertSQLException(() -> resultSet.getBoolean("ASK"));
    }

    @Test
    public void getBooleanBadColumn() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertSQLException(() -> resultSet.getBoolean("FOO"));
    }

    @Test
    public void testAsk() throws Exception {
        Utils.dumpToStdout(sampleResultSet());
        verify(lastBackendRequest).handle(Method.POST, sparql.urlPath(), null, Utils.TYPE_FORM_URLENCODED,
                Utils.queryParam("query", "ask{?s ?p ?o.}"));
    }

    @Test
    public void testAllClosed() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.close();
        assertSQLException(() -> resultSet.getBoolean("ASK"));
    }
}
