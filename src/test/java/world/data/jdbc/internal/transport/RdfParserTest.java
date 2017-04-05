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

import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static world.data.jdbc.model.LiteralFactory.createString;

public class RdfParserTest {

    @Rule
    public final CloserResource closer = new CloserResource();

    @Test
    public void testAccept() throws Exception {
        RdfParser parser = new RdfParser();
        assertThat(parser.getAcceptType()).isEqualTo(Utils.TYPE_RDF_JSON);
    }

    @Test
    public void testParser() throws Exception {
        RdfParser parser = new RdfParser();
        Response response = parser.parse(getResource("rdf-1.rj"));
        closer.register(response.getCleanup());

        assertThat(response.getBooleanResult()).isNull();
        assertThat(response.getColumns()).extracting("name").containsExactly("Subject", "Predicate", "Object");

        Iterator<Node[]> rows = response.getRows();
        assertThat(rows.next()).containsExactly(ex("about"), purl("title"), createString("Anna's Homepage", "en"));
        assertThat(rows.hasNext()).isFalse();
    }

    @Test
    public void testExample1() throws Exception {
        Iterator<Node[]> iter = parseResource("rdf-1.rj");
        assertThat(iter.next()).containsExactly(ex("about"), purl("title"), createString("Anna's Homepage", "en"));
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void testExample3() throws Exception {
        Iterator<Node[]> iter = parseResource("rdf-3.rj");
        assertThat(iter.next()).containsExactly(ex("about"), purl("title"), createString("Anna's Homepage", "en"));
        assertThat(iter.next()).containsExactly(ex("about"), purl("title"), createString("Annas hjemmeside", "da"));
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void testExample5() throws Exception {
        Iterator<Node[]> iter = parseResource("rdf-5.rj");
        assertThat(iter.next()).containsExactly(ex("about"), purl("title"),
                new Literal("<p xmlns=\"http://www.w3.org/1999/xhtml\"><b>Anna's</b> Homepage>/p>", rdf("XMLLiteral")));
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void testExample7() throws Exception {
        Iterator<Node[]> iter = parseResource("rdf-7.rj");
        assertThat(iter.next()).containsExactly(ex("about"), purl("creator"), new Blank("anna"));
        assertThat(iter.next()).containsExactly(new Blank("anna"), foaf("name"), createString("Anna"));
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void testExample9() throws Exception {
        Iterator<Node[]> iter = parseResource("rdf-9.rj");
        assertThat(iter.next()).containsExactly(new Blank("anna"), foaf("homepage"), ex("anna"));
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void testExample11() throws Exception {
        Iterator<Node[]> iter = parseResource("rdf-11.rj");
        assertThat(iter.next()).containsExactly(new Blank("anna"), foaf("name"), createString("Anna"));
        assertThat(iter.next()).containsExactly(new Blank("anna"), foaf("homepage"), ex("anna"));
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void testEmpty() throws Exception {
        Iterator<Node[]> iter = parse("{}");
        assertThat(iter.hasNext()).isFalse();
    }

    private Iri ex(String suffix) {
        return new Iri("http://example.org/" + suffix);
    }

    private Iri purl(String suffix) {
        return new Iri("http://purl.org/dc/terms/" + suffix);
    }

    private Iri rdf(String suffix) {
        return new Iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#" + suffix);
    }

    private Iri foaf(String suffix) {
        return new Iri("http://xmlns.com/foaf/0.1/" + suffix);
    }

    private InputStream getResource(String resourceName) throws Exception {
        return requireNonNull(getClass().getResourceAsStream(resourceName), resourceName);
    }

    private Iterator<Node[]> parseResource(String resourceName) throws Exception {
        URL resource = requireNonNull(getClass().getResource(resourceName), resourceName);
        return new TriplesParser(closer.register(ParserUtil.JSON_FACTORY.createParser(resource)));
    }

    private Iterator<Node[]> parse(String content) throws Exception {
        return new TriplesParser(closer.register(ParserUtil.JSON_FACTORY.createParser(content)));
    }
}
