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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import world.data.jdbc.model.Blank;
import world.data.jdbc.model.Iri;
import world.data.jdbc.model.Literal;
import world.data.jdbc.model.Node;
import world.data.jdbc.vocab.Xsd;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.internal.transport.ParserUtil.expect;
import static world.data.jdbc.internal.transport.ParserUtil.require;

/**
 * Parser for an RDF term within a document with media type
 * <a href="https://www.w3.org/TR/sparql11-results-json/#select-encode-terms">application/sparql-results+json</a>
 * or <a href="http://jena.apache.org/documentation/io/rdf-json.html">application/rdf+json</a>.
 */
final class NodeParser {
    private final JsonParser parser;
    private final boolean rdf;
    private boolean nonEmpty;
    private String type;
    private Iri datatype;
    private String value;
    private String language;

    /** Constructs a node parser for rdf terms w/'application/sparql-results+json'. */
    static NodeParser forSparqlResults(JsonParser parser) {
        return new NodeParser(parser, false);
    }

    /** Constructs a node parser for rdf object terms w/in 'application/rdf+json'. */
    static NodeParser forRdf(JsonParser parser) {
        return new NodeParser(parser, true);
    }

    private NodeParser(JsonParser parser, boolean rdf) {
        this.parser = requireNonNull(parser, "parser");
        this.rdf = rdf;
    }

    @Nullable
    Node[] parseRow(Map<String, Response.Column> columnsByVar) throws IOException {
        Node[] nodes = new Node[columnsByVar.size()];
        nonEmpty = false;
        FieldParser.parseObject(parser, (String field, JsonToken token) -> {
            Response.Column column = columnsByVar.get(field);
            if (column != null && token == JsonToken.START_OBJECT) {
                Node node = parse();
                nodes[column.getIndex()] = node;
                nonEmpty |= node != null;
            }
        });
        return nonEmpty ? nodes : null;
    }

    @Nullable
    Node parse() throws IOException {
        type = value = language = null;
        datatype = Xsd.STRING;
        FieldParser.parseObject(parser, (String field, JsonToken token) -> {
            switch (field) {
                case "type":
                    expect(parser, token, JsonToken.VALUE_STRING);
                    type = parser.getText();
                    break;
                case "value":
                    expect(parser, token, JsonToken.VALUE_STRING);
                    value = parser.getText();
                    break;
                case "datatype":
                    expect(parser, token, JsonToken.VALUE_STRING);
                    datatype = new Iri(parser.getText().intern());
                    break;
                case "lang":  // for application/rdf+json
                case "xml:lang":  // for application/sparql-results+json
                    expect(parser, token, JsonToken.VALUE_STRING);
                    language = parser.getText().intern();
                    break;
            }
        });
        switch (require(parser, type, "type")) {
            case "uri":
                return new Iri(require(parser, value, "value"));
            case "literal":
            case "typed-literal":
                return new Literal(require(parser, value, "value"), datatype, language);
            case "bnode":
                return new Blank(stripBlankPrefix(require(parser, value, "value")));
            default:
                return null;  // Ignore the node to maintain forward/backward compatibility
        }
    }

    private String stripBlankPrefix(String id) {
        // rdf+json includes _: prefix in blank node labels, sparql-results+json does not
        return rdf && id.startsWith("_:") ? id.substring(2) : id;
    }
}
