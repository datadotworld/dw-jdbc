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
package world.data.jdbc;

import fi.iki.elonen.NanoHTTPD;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import world.data.jdbc.connections.Connection;
import world.data.jdbc.statements.PreparedStatement;
import world.data.jdbc.statements.Statement;
import world.data.jdbc.testing.NanoHTTPDHandler;
import world.data.jdbc.testing.NanoHTTPDResource;
import world.data.jdbc.testing.SparqlHelper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static fi.iki.elonen.NanoHTTPD.Method.GET;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;

public class SparqlTest {

    private static NanoHTTPDHandler lastBackendRequest;
    private static String resultResourceName;
    private static String resultMimeType;

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
    public final SparqlHelper sparql = new SparqlHelper();

    @Before
    public void setup() {
        lastBackendRequest = mock(NanoHTTPDHandler.class);
    }

    @Test
    public void test() throws Exception {
        resultResourceName = "/select.json";
        resultMimeType = "application/json";

        Statement statement = sparql.createStatement(sparql.connect());
        ResultSet resultSet = sparql.executeQuery(statement, "select ?s ?p ?o where{?s ?p ?o.} limit 10");
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) {
                System.out.print(",  ");
            }
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println("");
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) {
                    System.out.print(",  ");
                }
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue);
            }
            System.out.println("");
        }
        verify(lastBackendRequest).handle(GET, "/sparql/dave/lahman-sabremetrics-dataset",
                "query=SELECT++%3Fs+%3Fp+%3Fo%0AWHERE%0A++%7B+%3Fs++%3Fp++%3Fo+%7D%0ALIMIT+++10%0A");
    }

    @Test
    public void testAsk() throws Exception {
        resultMimeType = "application/json";
        resultResourceName = "/ask.json";

        Statement statement = sparql.createStatement(sparql.connect());
        ResultSet resultSet = sparql.executeQuery(statement, "ask{?s ?p ?o.}");
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) {
                System.out.print(",  ");
            }
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println("");
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) {
                    System.out.print(",  ");
                }
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue);
            }
            System.out.println("");
        }
        verify(lastBackendRequest).handle(GET, "/sparql/dave/lahman-sabremetrics-dataset",
                "query=ASK%0AWHERE%0A++%7B+%3Fs++%3Fp++%3Fo+%7D%0A");
    }

    @Test
    public void testDescribe() throws Exception {
        resultResourceName = "/describe.xml";
        resultMimeType = "application/rdf+xml";

        Statement statement = sparql.createStatement(sparql.connect());
        ResultSet resultSet = sparql.executeQuery(statement, "DESCRIBE ?s where{?s ?p ?o.} limit 10");
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) {
                System.out.print(",  ");
            }
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println("");
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) {
                    System.out.print(",  ");
                }
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue);
            }
            System.out.println("");
        }
        verify(lastBackendRequest).handle(GET, "/sparql/dave/lahman-sabremetrics-dataset",
                "query=DESCRIBE+%3Fs%0AWHERE%0A++%7B+%3Fs++%3Fp++%3Fo+%7D%0ALIMIT+++10%0A");
    }

    @Test
    public void testConstruct() throws Exception {
        resultResourceName = "/construct.xml";
        resultMimeType = "application/rdf+xml";

        Statement statement = sparql.createStatement(sparql.connect());
        ResultSet resultSet = sparql.executeQuery(statement, "Construct{?o ?p ?s} where{?s ?p ?o.} limit 10");
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) {
                System.out.print(",  ");
            }
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println("");
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) {
                    System.out.print(",  ");
                }
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue);
            }
            System.out.println("");
        }
        verify(lastBackendRequest).handle(GET, "/sparql/dave/lahman-sabremetrics-dataset",
                "query=CONSTRUCT+%0A++%7B+%0A++++%3Fo+%3Fp+%3Fs+.%0A++%7D%0AWHERE%0A++%7B+%3Fs++%3Fp++%3Fo+%7D%0ALIMIT+++10%0A");
    }

    @Test
    public void testConstructTurtle() throws Exception {
        resultResourceName = "/construct.ttl";
        resultMimeType = "text/turtle";

        Statement statement = sparql.createStatement(sparql.connect());
        ResultSet resultSet = sparql.executeQuery(statement, "Construct{?o ?p ?s} where{?s ?p ?o.} limit 10");
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) {
                System.out.print(",  ");
            }
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println("");
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) {
                    System.out.print(",  ");
                }
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue);
            }
            System.out.println("");
        }
        verify(lastBackendRequest).handle(GET, "/sparql/dave/lahman-sabremetrics-dataset",
                "query=CONSTRUCT+%0A++%7B+%0A++++%3Fo+%3Fp+%3Fs+.%0A++%7D%0AWHERE%0A++%7B+%3Fs++%3Fp++%3Fo+%7D%0ALIMIT+++10%0A");
    }

    @Test
    public void testPrepared() throws Exception {
        resultResourceName = "/select.json";
        resultMimeType = "application/json";

        Connection connection = sparql.connect();
        PreparedStatement statement = sparql.prepareStatement(connection, "select ?s ?p ?o where{?s ?p ?o.} limit 10");
        ResultSet resultSet = sparql.executeQuery(statement);
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) {
                System.out.print(",  ");
            }
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println("");
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) {
                    System.out.print(",  ");
                }
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue);
            }
            System.out.println("");
        }
        verify(lastBackendRequest).handle(GET, "/sparql/dave/lahman-sabremetrics-dataset",
                "query=SELECT++%3Fs+%3Fp+%3Fo%0AWHERE%0A++%7B+%3Fs++%3Fp++%3Fo+%7D%0ALIMIT+++10%0A");
    }

    @Test
    public void testBadQuery() throws Exception {
        Statement statement = sparql.createStatement(sparql.connect());
        assertSQLException(() -> statement.executeQuery("select ?s ?p ?o where{?s ?p ?o.} limit ?"));
    }
}
