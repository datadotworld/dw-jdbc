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
import world.data.jdbc.internal.util.AbstractIterator;
import world.data.jdbc.model.Node;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.internal.transport.ParserUtil.expect;

/**
 * Streaming parser for the 'results.bindings' array in a response with media type 'application/sparql-results+json'.
 * <p>
 * The caller is responsible for closing the {@link JsonParser} when iteration is complete.
 */
final class BindingsParser extends AbstractIterator<Node[]> {
    private final JsonParser parser;
    private final Map<String, Response.Column> columnsByVar;
    private final NodeParser nodeParser;

    BindingsParser(JsonParser parser, Map<String, Response.Column> columnsByVar) throws IOException {
        this.parser = requireNonNull(parser, "parser");
        this.columnsByVar = requireNonNull(columnsByVar, "columnsByVar");
        this.nodeParser = NodeParser.forSparqlResults(parser);
        expect(parser, parser.getCurrentToken(), JsonToken.START_ARRAY);
    }

    @Override
    protected Node[] computeNext() {
        try {
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                Node[] nodes = nodeParser.parseRow(columnsByVar);
                if (nodes != null) {
                    return nodes;
                }
            }
            // At this point we should be right near the end of the result set, all that's left is to consume
            // the closing '}' and ']' characters.  Consume the closing characters to the end of the stream since
            // http libraries may use "read to eof" to indicate that the underlying socket associated with the
            // input stream may be reused for another http request (ie. keep-alive).  But if we encounter something
            // unexpected than stop and abandon the input stream--we don't want to read an unbounded amount of data.
            while (parser.hasCurrentToken() && parser.getCurrentToken().isStructEnd()) {
                parser.nextToken();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return endOfData();
    }
}
