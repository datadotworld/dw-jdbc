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
import world.data.jdbc.DataWorldConnection;
import world.data.jdbc.DataWorldStatement;
import world.data.jdbc.JdbcCompatibility;
import world.data.jdbc.testing.NanoHTTPDHandler;
import world.data.jdbc.testing.NanoHTTPDResource;
import world.data.jdbc.testing.SparqlHelper;
import world.data.jdbc.testing.Utils;

import java.sql.ResultSet;
import java.sql.SQLException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;

public class SelectResultSetTest {
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
        return sparql.executeQuery(statement, "select ?s where {?s ?p ?o.}");
    }

    @Test
    public void findColumn() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.findColumn("s")).isEqualTo(1);
    }

    @Test
    public void findColumnUnknown() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertSQLException(() -> resultSet.findColumn("q"));
    }

    @Test
    public void getNotAtRow() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertSQLException(() -> resultSet.getURL(1));
    }

    @Test
    public void getUrlOOBE() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertSQLException(() -> resultSet.getURL(5));
    }

    @Test
    public void getUrl() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getURL(1).toString()).isEqualTo("http://data.world/user8/lahman-baseball/");
    }

    @Test
    public void getUrlByName() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getURL("s").toString()).isEqualTo("http://data.world/user8/lahman-baseball/");
    }

    @Test
    public void getUrlBadName() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertSQLException(() -> resultSet.getURL("Q"));
    }

    @Test
    public void getMetaData() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(3);
    }

    @Test
    public void getMetaDataLow() throws Exception {
        DataWorldConnection connection = sparql.connect();
        connection.setJdbcCompatibilityLevel(JdbcCompatibility.LOW);
        DataWorldStatement statement = sparql.createStatement(connection);
        ResultSet resultSet = sparql.executeQuery(statement, "select ?s where {?s ?p ?o.}");
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(3);
    }

    @Test
    public void getMetaDataHigh() throws Exception {
        DataWorldConnection connection = sparql.connect();
        connection.setJdbcCompatibilityLevel(JdbcCompatibility.HIGH);
        DataWorldStatement statement = sparql.createStatement(connection);
        ResultSet resultSet = sparql.executeQuery(statement, "select ?s where {?s ?p ?o.}");
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(3);
    }

    @Test
    public void testAllClosed() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        resultSet.close();
        assertSQLException(() -> resultSet.getURL(1));
        assertSQLException(() -> resultSet.findColumn("s"));
    }
}
