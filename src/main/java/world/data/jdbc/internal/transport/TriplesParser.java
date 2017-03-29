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
import world.data.jdbc.model.Blank;
import world.data.jdbc.model.Iri;
import world.data.jdbc.model.Node;

import java.io.IOException;
import java.io.UncheckedIOException;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.internal.transport.ParserUtil.expect;

/**
 * Streaming parser for json triples in
 * <a href="https://www.w3.org/TR/2013/NOTE-rdf-json-20131107/">application/rdf+json</a> format.
 * <p>
 * This is a very simple format, roughly:
 * <pre>
 * { "S" : { "P" : [ O ] } }
 * </pre>
 * where {@code "S"} and {@code "P"} are URI or blank node strings and {@code O} is a json object similar to
 * the one used for RDF terms in 'application/sparql-results+json'.
 * <p>
 * The caller is responsible for closing the {@link JsonParser} when iteration is complete.
 */
final class TriplesParser extends AbstractIterator<Node[]> {
    private final JsonParser parser;
    private final NodeParser objectParser;
    private int level;
    private Node subject;
    private Node predicate;

    TriplesParser(JsonParser parser) throws IOException {
        this.parser = requireNonNull(parser, "parser");
        this.objectParser = NodeParser.forRdf(parser);
    }

    @Override
    protected Node[] computeNext() {
        // This is a basically 4 nested loops (for each top-level, subject, predicate, object...) where control flow
        // has been flattened out to allow returning results in a streaming fashion.
        try {
            // Loop until we find a complete Triple or end-of-data.
            for (; ; ) {
                dispatch:
                switch (level) {
                    // Within a subject+predicate array, parse the next object
                    case 3:
                        JsonToken subjectToken;
                        while ((subjectToken = parser.nextToken()) != JsonToken.END_ARRAY) {
                            if (subjectToken == JsonToken.START_OBJECT) {
                                Node object = objectParser.parse();
                                expect(parser, parser.getCurrentToken(), JsonToken.END_OBJECT);
                                if (subject == null || predicate == null || object == null) {
                                    throw new IllegalStateException();  // sanity check
                                }
                                return new Node[]{subject, predicate, object};
                            } else {
                                parser.skipChildren();
                            }
                        }
                        // At the end of the subject+predicate array, move to the next predicate
                        predicate = null;
                        level = 2;
                        // Fall through

                    case 2:
                        // Within a subject object, parse the next predicate
                        String predicateString;
                        while ((predicateString = parser.nextFieldName()) != null) {
                            if (parser.nextToken() == JsonToken.START_ARRAY) {
                                predicate = parseSubjectOrPredicate(predicateString);
                                level = 3;
                                break dispatch;
                            } else {
                                parser.skipChildren();
                            }
                        }
                        expect(parser, parser.getCurrentToken(), JsonToken.END_OBJECT);
                        // At the end of the subject object, move to the next subject
                        subject = null;
                        level = 1;
                        // Fall through

                    case 1:
                        // Within the top-level object, parse the next subject
                        String subjectString;
                        while ((subjectString = parser.nextFieldName()) != null) {
                            if (parser.nextToken() == JsonToken.START_OBJECT) {
                                subject = parseSubjectOrPredicate(subjectString);
                                level = 2;
                                break dispatch;
                            } else {
                                parser.skipChildren();
                            }
                        }
                        expect(parser, parser.getCurrentToken(), JsonToken.END_OBJECT);
                        level = 0;
                        // Fall through

                    case 0:
                        // Top-level object (START HERE!)
                        if (parser.nextToken() == JsonToken.START_OBJECT) {
                            level = 1;
                            break;
                        }
                        return endOfData();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Node parseSubjectOrPredicate(String string) {
        if (string.startsWith("_:")) {
            return new Blank(string.substring(2));
        } else {
            return new Iri(string);
        }
    }
}
