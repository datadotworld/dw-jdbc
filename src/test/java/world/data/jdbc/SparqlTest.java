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
import fi.iki.elonen.NanoHTTPD.Response.Status;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import world.data.jdbc.testing.NanoHTTPDHandler;
import world.data.jdbc.testing.NanoHTTPDResource;
import world.data.jdbc.testing.SparqlHelper;
import world.data.jdbc.testing.Utils;

import java.net.URL;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;

public class SparqlTest {

    private static NanoHTTPDHandler lastBackendRequest;
    private static String resultResourceName;
    private static String resultMimeType;
    private static boolean badRequest;

    @ClassRule
    public static final NanoHTTPDResource proxiedServer = new NanoHTTPDResource(3333) {
        @Override
        protected NanoHTTPD.Response serve(NanoHTTPD.IHTTPSession session) throws Exception {
            if (badRequest) {
                return newResponse(Status.BAD_REQUEST, "application/json", "{}");
            }
            String authorization = session.getHeaders().get("authorization");
            if (!"Bearer access-token".equals(authorization)) {
                return newResponse(Status.UNAUTHORIZED, "text/plain", "Missing or incorrect password");
            }
            NanoHTTPDHandler.invoke(session, lastBackendRequest);
            URL source = requireNonNull(getClass().getResource(resultResourceName), resultResourceName);
            return newResponse(Status.OK, resultMimeType, IOUtils.toString(source, UTF_8));
        }
    };

    @Rule
    public final SparqlHelper sparql = new SparqlHelper();

    @Before
    public void setup() {
        lastBackendRequest = mock(NanoHTTPDHandler.class);
        badRequest = false;
    }

    @Test
    public void test() throws Exception {
        resultResourceName = "/select.json";
        resultMimeType = Utils.TYPE_SPARQL_RESULTS;

        DataWorldStatement statement = sparql.createStatement(sparql.connect());
        Utils.dumpToStdout(sparql.executeQuery(statement, "select ?s ?p ?o where{?s ?p ?o.} limit 10"));
        verify(lastBackendRequest).handle(Method.POST, sparql.urlPath(), null, Utils.TYPE_FORM_URLENCODED,
                Utils.queryParam("query", "select ?s ?p ?o where{?s ?p ?o.} limit 10"));
    }

    @Test
    public void testAsk() throws Exception {
        resultResourceName = "/ask.json";
        resultMimeType = Utils.TYPE_SPARQL_RESULTS;

        DataWorldStatement statement = sparql.createStatement(sparql.connect());
        Utils.dumpToStdout(sparql.executeQuery(statement, "ask{?s ?p ?o.}"));
        verify(lastBackendRequest).handle(Method.POST, sparql.urlPath(), null, Utils.TYPE_FORM_URLENCODED,
                Utils.queryParam("query", "ask{?s ?p ?o.}"));
    }

    @Test
    public void testDescribe() throws Exception {
        resultResourceName = "/describe.rj";
        resultMimeType = Utils.TYPE_RDF_JSON;

        DataWorldStatement statement = sparql.createStatement(sparql.connect());
        Utils.dumpToStdout(sparql.executeQuery(statement, "DESCRIBE ?s where{?s ?p ?o.} limit 10"));
        verify(lastBackendRequest).handle(Method.POST, sparql.urlPath(), null, Utils.TYPE_FORM_URLENCODED,
                Utils.queryParam("query", "DESCRIBE ?s where{?s ?p ?o.} limit 10"));
    }

    @Test
    public void testConstruct() throws Exception {
        resultResourceName = "/construct.rj";
        resultMimeType = Utils.TYPE_RDF_JSON;

        DataWorldStatement statement = sparql.createStatement(sparql.connect());
        Utils.dumpToStdout(sparql.executeQuery(statement, "Construct{?o ?p ?s} where{?s ?p ?o.} limit 10"));
        verify(lastBackendRequest).handle(Method.POST, sparql.urlPath(), null, Utils.TYPE_FORM_URLENCODED,
                Utils.queryParam("query", "Construct{?o ?p ?s} where{?s ?p ?o.} limit 10"));
    }

    @Test
    public void testPrepared() throws Exception {
        resultResourceName = "/select.json";
        resultMimeType = Utils.TYPE_SPARQL_RESULTS;

        DataWorldConnection connection = sparql.connect();
        DataWorldPreparedStatement statement = sparql.prepareStatement(connection, "select ?s ?p ?o where{?s ?p ?o.} limit 10");
        Utils.dumpToStdout(sparql.executeQuery(statement));
        verify(lastBackendRequest).handle(Method.POST, sparql.urlPath(), null, Utils.TYPE_FORM_URLENCODED,
                Utils.queryParam("query", "select ?s ?p ?o where{?s ?p ?o.} limit 10"));
    }

    @Test
    public void testBadQuery() throws Exception {
        badRequest = true;

        DataWorldStatement statement = sparql.createStatement(sparql.connect());
        assertSQLException(() -> statement.executeQuery("select ?s ?p ?o where {?s ?p ?o.} limit ?"));
    }
}
