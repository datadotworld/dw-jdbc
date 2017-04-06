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

import org.junit.Test;

import java.io.InputStream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class ErrorMessageParserTest {

    @Test
    public void testJson() throws Exception {
        // Use an incorrect contentType to simulate buggy back-end
        String message = parseResource("error-message.json", "application/sparql-results+json");
        assertThat(message).isEqualTo("No such entity exists.");
    }

    @Test
    public void testText() throws Exception {
        // Use an incorrect contentType to simulate buggy back-end
        String message = parseResource("error-message.txt", "application/sparql-results+json");
        assertThat(message).isEqualTo("Lexical error at line 1, column 40.  Encountered: \" \" (32), after : \"limti\"");
    }

    @Test
    public void testHtml() throws Exception {
        String message = parseResource("error-message.html", "text/html");
        assertThat(message).isNull();
    }

    private String parseResource(String resourceName, String contentType) throws Exception {
        try (InputStream in = getResource(resourceName)) {
            return new ErrorMessageParser().parse(in, contentType);
        }
    }

    private InputStream getResource(String resourceName) throws Exception {
        return requireNonNull(getClass().getResourceAsStream(resourceName), resourceName);
    }
}
