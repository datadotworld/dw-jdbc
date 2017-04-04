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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.internal.transport.ParserUtil.expect;
import static world.data.jdbc.internal.transport.ParserUtil.require;

/**
 * Parses data.world column metadata from a result set.
 */
final class ColumnParser {
    private final JsonParser parser;
    private String name;

    ColumnParser(JsonParser parser) {
        this.parser = requireNonNull(parser, "parser");
    }

    List<Response.Column> parseArray() throws IOException {
        List<Response.Column> columns = new ArrayList<>();
        ElementParser.parseArray(parser, (JsonToken metadataToken) -> {
            if (metadataToken == JsonToken.START_OBJECT) {
                columns.add(parse(columns.size()));
            }
        });
        return columns;
    }

    private Response.Column parse(int index) throws IOException {
        Response.Column.Builder builder = Response.Column.builder().index(index);
        name = null;
        FieldParser.parseObject(parser, (String field, JsonToken token) -> {
            switch (field) {
                case "name":
                    expect(parser, token, JsonToken.VALUE_STRING);
                    builder.name(name = parser.getText());
                    break;
                case "description":
                    builder.description(parser.getText());
                    break;
                case "type":
                    builder.datatypeIri(parser.getText());
                    break;
                case "formatString":
                    builder.formatString(parser.getText());
                    break;
                case "units":
                    builder.units(parser.getText());
                    break;
                case "scalingFactor":
                    if (token.isScalarValue()) {
                        builder.scalingFactor(parser.getValueAsDouble());
                    }
                    break;
            }
        });
        require(parser, name, "name");
        return builder.build();
    }
}
