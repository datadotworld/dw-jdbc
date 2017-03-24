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
package world.data.jdbc.statements;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.junit.Test;

import static org.apache.jena.graph.NodeFactory.createBlankNode;
import static org.apache.jena.graph.NodeFactory.createLiteral;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class NTripleFormatTest {
    static {
        ARQ.init();
    }

    @Test
    public void testParseIRI() {
        doTestValid("<http://example.com#p>", createURI("http://example.com#p"));
    }

    @Test
    public void testParseBlankNode() {
        doTestValid("_:a", createBlankNode("a"), "_:Ba");
    }

    @Test
    public void testParseString() {
        doTestValid("\"hel\\\\o\\twørld \\\"\r\\n\"", createLiteral("hel\\o\twørld \"\r\n"), "\"hel\\\\o\\twørld \\\"\\r\\n\"");
    }

    @Test
    public void testParseStringWithLanguage() {
        doTestValid("\"foo\"@en", createLiteral("foo", "en"));
    }

    @Test
    public void testParseStringWithType() {
        doTestValid("\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>", createLiteral("3", XSDDatatype.XSDinteger));
    }

    @Test
    public void testParseStringWithCustomType() {
        RDFDatatype dt = TypeMapper.getInstance().getSafeTypeByName("http://example.com#type");
        doTestValid("\"3\"^^<http://example.com#type>", createLiteral("3", dt));
    }

    @Test
    public void testParseEmpty() {
        doTestInvalid("");
    }

    @Test
    public void testParseBlank() {
        doTestInvalid("  \t  ");
    }

    @Test
    public void testParseTrailing() {
        doTestInvalid("\"foo\" \"bar\"");
    }

    @Test
    public void testParseDot() {
        doTestInvalid(".");
    }

    @Test
    public void testParseNumber() {
        doTestInvalid("3");
        doTestInvalid("3.1");
    }

    @Test
    public void testParseBoolean() {
        doTestInvalid("false");
        doTestInvalid("true");
    }

    @Test
    public void testParseSingleQuotes() {
        doTestInvalid("'abc'");
    }

    @Test
    public void testParsePlainURI() {
        doTestInvalid("http://www.w3.org/2001/XMLSchema#integer");  // looks like a prefixed name
    }

    @Test
    public void testParseBadLanguage() {
        doTestInvalid("\"abc\"@\"en\"");
    }

    @Test
    public void testParseBadType() {
        doTestInvalid("\"abc\"^^http://www.w3.org/2001/XMLSchema#integer");
    }

    @Test
    public void testParseVariable() {
        doTestInvalid("?x");
    }

    @Test
    public void testParsePrefixed() {
        doTestInvalid("xsd:int");
    }

    @Test
    public void testParseWord() {
        doTestInvalid("word");
    }

    private void doTestValid(String string, Node expected) {
        // Except for preserving unnecessary escapes, string should round trip back to their original form
        doTestValid(string, expected, string);
    }

    private void doTestValid(String string, Node expected, String expectedRoundTrip) {
        Node actual = NTripleFormat.parseNode(string);
        assertThat(actual).isEqualTo(expected);
        assertThat(NTripleFormat.toString(actual)).isEqualTo(expectedRoundTrip);
    }

    private void doTestInvalid(String string) {
        assertThatThrownBy(() -> NTripleFormat.parseNode(string))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid NTriple-encoded value: " + string);
    }
}
