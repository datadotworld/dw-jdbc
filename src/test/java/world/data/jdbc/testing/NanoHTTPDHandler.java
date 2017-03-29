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
package world.data.jdbc.testing;

import fi.iki.elonen.NanoHTTPD.IHTTPSession;
import fi.iki.elonen.NanoHTTPD.Method;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.UncheckedIOException;

import static java.nio.charset.StandardCharsets.UTF_8;

public interface NanoHTTPDHandler {
    void handle(Method method, String urlPath, String queryParameters);

    void handle(Method method, String urlPath, String queryParameters, String contentType, String body);

    static void invoke(IHTTPSession session, NanoHTTPDHandler handler) {
        Method method = session.getMethod();
        String urlPath = session.getUri();
        String queryParameters = session.getQueryParameterString();
        if (method == Method.POST || method == Method.PUT) {
            String body = null;
            String contentType = session.getHeaders().get("content-type");
            String requestLength = session.getHeaders().get("content-length");
            if (requestLength != null) {
                byte[] bytes = new byte[Integer.parseInt(requestLength)];
                try {
                    IOUtils.readFully(session.getInputStream(), bytes);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                body = new String(bytes, UTF_8);
            }
            handler.handle(method, urlPath, queryParameters, contentType, body);
        } else {
            handler.handle(method, urlPath, queryParameters);
        }
    }
}
