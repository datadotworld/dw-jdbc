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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import lombok.experimental.UtilityClass;

/**
 * Helpers for parsing json.
 */
@UtilityClass
final class ParserUtil {

    static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)  // streaming, no point...
            .enable(JsonParser.Feature.ALLOW_COMMENTS);  // convenient for tests that mock json responses...

    static void expect(JsonParser parser, JsonToken actual, JsonToken expected) throws JsonParseException {
        if (actual != expected) {
            throw new JsonParseException(parser, String.format("Expected %s, found %s", expected, actual));
        }
    }

    static <T> T require(JsonParser parser, T value, String property) throws JsonParseException {
        if (value == null) {
            throw new JsonParseException(parser, String.format("Missing required '%s' property", property));
        }
        return value;
    }
}
