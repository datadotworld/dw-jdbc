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

import static world.data.jdbc.internal.transport.ParserUtil.expect;

/**
 * Helper for parsing a sequence of json array elements.
 */
@FunctionalInterface
interface ElementParser {

    void parse(JsonToken token) throws IOException;

    static void parseArray(JsonParser parser, ElementParser elementParser) throws IOException {
        expect(parser, parser.getCurrentToken(), JsonToken.START_ARRAY);
        JsonToken token;
        while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
            elementParser.parse(token);
            // Skip unconsumed child arrays/objects, for backwards/forwards compatibility
            parser.skipChildren();
        }
    }
}
