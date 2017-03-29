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
package world.data.jdbc.internal.transport;

import org.junit.Rule;
import org.junit.Test;
import world.data.jdbc.model.Blank;
import world.data.jdbc.model.Iri;
import world.data.jdbc.model.Literal;
import world.data.jdbc.model.Node;
import world.data.jdbc.testing.CloserResource;
import world.data.jdbc.testing.Utils;
import world.data.jdbc.vocab.Xsd;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static world.data.jdbc.model.LiteralFactory.createDecimal;
import static world.data.jdbc.model.LiteralFactory.createInteger;
import static world.data.jdbc.model.LiteralFactory.createString;

public class SparqlResultsParserTest {

    @Rule
    public final CloserResource closer = new CloserResource();

    @Test
    public void testAccept() {
        SparqlResultsParser parser = new SparqlResultsParser();
        assertThat(parser.getAcceptType()).isEqualTo(Utils.TYPE_SPARQL_RESULTS);
    }

    @Test
    public void testParser() throws Exception {
        Response response = parse("sparql-results-1.srj");

        assertThat(response.getCleanup()).isNotNull();
        assertThat(response.getBooleanResult()).isNull();
        assertThat(response.getColumns()).extracting("name")
                .containsExactly("x", "hpage", "name", "mbox", "age", "blurb", "friend");

        Iterator<Node[]> rows = response.getRows();
        assertThat(rows.next()).containsExactly(new Blank("r1"), ex("alice/"), createString("Alice"), createString(""), null,
                new Literal("<p xmlns=\"http://www.w3.org/1999/xhtml\">My name is <b>alice</b></p>", rdf("XMLLiteral")),
                new Blank("r2"));
        assertThat(rows.next()).containsExactly(new Blank("r2"), ex("bob/"), createString("Bob", "en"),
                new Iri("mailto:bob@work.example.org"), null, null, new Blank("r1"));
        assertThat(rows.hasNext()).isFalse();
    }

    @Test
    public void testEmptyA() throws Exception {
        Response response = parse("sparql-results-empty-A.srj");

        assertThat(response.getCleanup()).isNotNull();
        assertThat(response.getBooleanResult()).isNull();
        assertThat(response.getColumns()).extracting("name").containsExactly(" ");
        assertThat(response.getRows().hasNext()).isFalse();
    }

    @Test
    public void testEmptyB() throws Exception {
        Response response = parse("sparql-results-empty-B.srj");

        assertThat(response.getCleanup()).isNotNull();
        assertThat(response.getBooleanResult()).isNull();
        assertThat(response.getColumns()).extracting("name").containsExactly(" ");
        assertThat(response.getRows().hasNext()).isFalse();
    }

    @Test
    public void testAsk() throws Exception {
        Response response = parse("/ask.json");
        assertThat(response.getCleanup()).isNull();
        assertThat(response.getBooleanResult()).isTrue();
        assertThat(response.getColumns()).isNull();
        assertThat(response.getRows()).isNull();
    }

    @Test
    public void testSelectEnhanced() throws Exception {
        Response response = parse("/select-sql.json");
        assertThat(response.getCleanup()).isNotNull();
        assertThat(response.getBooleanResult()).isNull();

        List<Response.Column> columns = response.getColumns();
        assertThat(columns).extracting("name").containsExactly("yearID", "teamID", "Rank", "FP", "name", "park");
        assertThat(columns).extracting("datatypeIri").containsExactly(expand("xsd:integer"), expand("xsd:string"), expand("xsd:integer"), expand("xsd:decimal"), expand("xsd:string"), expand("xsd:string"));
        assertThat(columns).extracting("formatString").containsExactly("0000", null, null, null, null, null);

        Iterator<Node[]> rows = response.getRows();
        assertThat(rows.next()).containsExactly(createInteger(1871), createString("BS1"), createInteger(3),
                createDecimal(BigDecimal.valueOf(0.83)), createString("Boston Red Stockings"), createString("South End Grounds I"));
        assertThat(rows).hasSize(9);  // 9 more rows
    }

    private Iri ex(String suffix) {
        return new Iri("http://work.example.org/" + suffix);
    }

    private Iri rdf(String suffix) {
        return new Iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#" + suffix);
    }

    private String expand(String uri) {
        return uri.startsWith("xsd:") ? Xsd.NS + uri.substring(4) : uri;
    }

    private InputStream getResource(String resourceName) throws Exception {
        return requireNonNull(getClass().getResourceAsStream(resourceName), resourceName);
    }

    private Response parse(String resourceName) throws Exception {
        SparqlResultsParser parser = new SparqlResultsParser();
        Response response = parser.parse(getResource(resourceName));
        if (response.getCleanup() != null) {
            closer.register(response.getCleanup());
        }
        return response;
    }
}
