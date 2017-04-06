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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;
import static world.data.jdbc.internal.transport.ParserUtil.expect;

/**
 * Parses a json error message object containing the following fields:
 * <ul>
 * <li>{@code code} - http status code</li>
 * <li>{@code message} - human-readable error message</li>
 * <li>{@code details} - low-level debugging details</li>
 * </ul>
 */
final class ErrorMessageParser implements StreamParser<String> {
    private String message;

    @Override
    public String getAcceptType() {
        return "*/*";
    }

    @Override
    public String parse(InputStream in, String contentType) throws Exception {
        // Don't trust 'contentType' for error messages, they're hard to get right, proxy servers can implement
        // their own error pages (for eg. 502, 503, 504).  The server *should* return a json-formatted error message
        // object but doesn't always.

        // If html, most likely a 502, 503, 504 error page from a proxy server / load balancer.  The http
        // message associated with the error code is likely sufficient and the html not appropriate.
        if ("text/html".equals(contentType)) {
            return null;
        }

        // From now on, ignore 'contentType' and detect json error messages by '{' as the first character.
        if (!in.markSupported()) {
            in = new BufferedInputStream(in);
        }
        in.mark(1);
        int firstChar = in.read();
        in.reset();
        if (firstChar == '{') {
            return parseJson(in);
        } else {
            return parsePlainText(in);
        }
    }

    /**
     * Parses a json error message object containing the following fields:
     * <ul>
     * <li>{@code code} - http status code</li>
     * <li>{@code message} - human-readable error message</li>
     * <li>{@code details} - low-level debugging details</li>
     * </ul>
     */
    private String parseJson(InputStream in) throws IOException {
        message = null;
        try (JsonParser parser = ParserUtil.JSON_FACTORY.createParser(in)) {
            expect(parser, parser.nextToken(), JsonToken.START_OBJECT);
            FieldParser.parseObject(parser, (String field, JsonToken token) -> {
                if ("message".equals(field)) {
                    message = parser.getText();
                }
            });
        }
        return message;
    }

    private String parsePlainText(InputStream in) throws IOException {
        return readString(in, UTF_8).trim();
    }

    private String readString(InputStream in, Charset charset) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        copy(in, buf);
        return buf.toString(charset.name());
    }

    private void copy(InputStream in, OutputStream out) throws IOException {
        byte[] b = new byte[4096];
        int len;
        while ((len = in.read(b)) != -1) {
            out.write(b, 0, len);
        }
    }
}
