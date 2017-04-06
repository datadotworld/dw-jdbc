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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import world.data.jdbc.model.Node;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static world.data.jdbc.internal.transport.ParserUtil.expect;

/**
 * Parses a {@link Response} object in data.world-extended
 * <a href="https://www.w3.org/TR/sparql11-results-json/">application/sparql-results+json</a> format.
 */
final class SparqlResultsParser implements StreamParser<Response> {

    @Override
    public String getAcceptType() {
        return "application/sparql-results+json";
    }

    @Override
    public Response parse(InputStream in, String contentType) throws IOException {
        JsonParser parser = ParserUtil.JSON_FACTORY.createParser(in);
        List<Response.Column> columns = null;
        List<String> variables = new ArrayList<>();
        boolean[] foundVariables = {false};
        boolean foundBindings = false;
        expect(parser, parser.nextToken(), JsonToken.START_OBJECT);
        String section;
        outer:
        while ((section = parser.nextFieldName()) != null) {
            JsonToken sectionToken = parser.nextToken();
            switch (section) {
                case "metadata":
                    if (sectionToken == JsonToken.START_ARRAY) {
                        columns = new ColumnParser(parser).parseArray();
                    }
                    break;
                case "head":
                    if (sectionToken == JsonToken.START_OBJECT) {
                        FieldParser.parseObject(parser, (String headField, JsonToken headToken) -> {
                            if ("vars".equals(headField) && headToken == JsonToken.START_ARRAY) {
                                ElementParser.parseArray(parser, (JsonToken token) -> {
                                    if (token.isScalarValue()) {
                                        variables.add(parser.getText());
                                    }
                                });
                                foundVariables[0] = true;
                            }
                        });
                    }
                    break;
                case "results":
                    if (sectionToken == JsonToken.START_OBJECT) {
                        String resultsField;
                        while ((resultsField = parser.nextFieldName()) != null) {
                            JsonToken resultsToken = parser.nextToken();
                            if ("bindings".equals(resultsField) && resultsToken == JsonToken.START_ARRAY) {
                                foundBindings = true;
                                break outer;
                            }
                            // Skip unconsumed child arrays/objects, for backwards/forwards compatibility
                            parser.skipChildren();
                        }
                        expect(parser, parser.getCurrentToken(), JsonToken.END_OBJECT);
                    }
                    break;
                case "boolean":
                    boolean askResult = parser.getBooleanValue();
                    // Consume the rest of the token stream (only if we're near the end) and close the parser.
                    //noinspection StatementWithEmptyBody
                    while (parser.nextToken() != null && parser.getCurrentToken().isStructEnd()) {
                    }
                    parser.close();
                    return Response.builder()
                            .booleanResult(askResult)
                            .build();
            }
            // Skip unconsumed child arrays/objects, for backwards/forwards compatibility
            parser.skipChildren();
        }
        if (!foundVariables[0]) {
            throw new JsonParseException(parser, "Missing required 'head.vars' array in result set response.");
        } else if (!foundBindings) {
            throw new JsonParseException(parser, "Missing required 'results.bindings' array in result set response.");
        } else if (columns != null && columns.size() != variables.size()) {
            throw new JsonParseException(parser, "Expected 'metadata' and 'head.vars' arrays to have the same number of elements.");
        } else if (columns == null) {
            // If the extended column metadata is missing then fake it from the variables array
            columns = new ArrayList<>();
            for (String variable : variables) {
                columns.add(Response.Column.builder().index(columns.size()).name(variable).build());
            }
        }
        Map<String, Response.Column> columnsByVar = new LinkedHashMap<>();
        for (int i = 0; i < variables.size(); i++) {
            columnsByVar.put(variables.get(i), columns.get(i));
        }
        Iterator<Node[]> rows = new BindingsParser(parser, columnsByVar);
        return Response.builder()
                .columns(columns)
                .rows(rows)
                .cleanup(parser)
                .build();
    }
}
