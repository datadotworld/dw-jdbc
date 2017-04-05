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

import world.data.jdbc.internal.util.CloseableRef;
import world.data.jdbc.model.Node;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static world.data.jdbc.internal.util.Conditions.check;
import static world.data.jdbc.internal.util.Optionals.or;

/**
 * The class that actually executes HTTP requests against a remote data.world query server.
 */
public final class HttpQueryApi implements QueryApi {

    // Order the response parsers from most to least desirable for content-type negotiation
    private static final List<StreamParser<Response>> STANDARD_PARSERS = Arrays.asList(
            new RdfParser(),  // Sparql DESCRIBE+CONSTRUCT
            new SparqlResultsParser());  // SQL or Sparql SELECT+ASK

    private final URL queryEndpoint;
    private final String userAgent;
    private final String authToken;

    public HttpQueryApi(URL queryEndpoint, String userAgent, String authToken) {
        this.queryEndpoint = requireNonNull(queryEndpoint, "queryEndpoint");
        this.userAgent = requireNonNull(userAgent, "userAgent");
        this.authToken = authToken;
    }

    @Override
    public Response executeQuery(String query, Map<String, Node> parameters, Integer timeoutSeconds) throws SQLException {
        requireNonNull(query, "query");
        requireNonNull(parameters, "parameters");

        // Construct the request params
        Map<String, String> requestParams = new LinkedHashMap<>();
        requestParams.put("query", query);
        for (Map.Entry<String, Node> entry : parameters.entrySet()) {
            String name = entry.getKey();
            Node value = entry.getValue();
            check(name.startsWith("$") && name.length() > 1, "Illegal parameter name: %s", name);
            if (value != null) {
                requestParams.put(name, value.toString());
            }
        }

        // Execute the request
        return post(requestParams, timeoutSeconds, STANDARD_PARSERS);
    }

    private <T> T post(Map<String, String> requestParams, Integer timeoutSeconds,
                       List<StreamParser<T>> responseParsers) throws SQLException {
        try {
            // Build the form-encoded request body
            StringBuilder buf = new StringBuilder();
            for (Map.Entry<String, String> entry : requestParams.entrySet()) {
                if (buf.length() > 0) {
                    buf.append('&');
                }
                buf.append(encode(entry.getKey())).append('=').append(encode(entry.getValue()));
            }
            byte[] requestBody = buf.toString().getBytes(StandardCharsets.UTF_8);

            String acceptTypes = responseParsers.stream()
                    .map(StreamParser::getAcceptType)
                    .collect(joining(", "));

            // Setup the request
            int readTimeout = Math.min(or(timeoutSeconds, 60), 60);
            int connectTimeout = Math.min(readTimeout, 5);
            HttpURLConnection connection = (HttpURLConnection) queryEndpoint.openConnection();
            connection.setConnectTimeout((int) TimeUnit.SECONDS.toMillis(connectTimeout));
            connection.setReadTimeout((int) TimeUnit.SECONDS.toMillis(readTimeout));
            connection.addRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
            connection.addRequestProperty("Accept", acceptTypes);
            connection.addRequestProperty("Accept-Encoding", "gzip");
            connection.addRequestProperty("User-Agent", userAgent);
            if (authToken != null) {
                connection.addRequestProperty("Authorization", "Bearer " + authToken);
            }

            // Send the request
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setFixedLengthStreamingMode(requestBody.length);
            connection.getOutputStream().write(requestBody);

            // Wait for the response
            int status = connection.getResponseCode();
            String message = connection.getResponseMessage();

            // Check for errors, eg. 401 Unauthorized etc.
            if (status >= 400) {
                new CloseableRef(connection.getErrorStream()).close();
                throw new SQLException(String.format("HTTP request to '%s' failed with response %d: %s", queryEndpoint, status, message));
            }

            // This endpoint isn't expected to return redirects or other 2xx, 3xx responses
            if (status != 200) {
                throw new SQLException(String.format("HTTP request to '%s' failed with unexpected response %d: %s", queryEndpoint, status, message));
            }

            // Once we've checked that status is 2xx or 3xx it's safe to get the InputStream
            InputStream in = connection.getInputStream();
            try (CloseableRef cleanup = new CloseableRef(in)) {

                // Decompress the response, if necessary
                if ("gzip".equals(trimHeader(connection.getHeaderField("Content-Encoding")))) {
                    in = cleanup.set(new GZIPInputStream(new BufferedInputStream(in)));
                }

                // Pick a parser based on the content type returned.  The parser becomes responsible for closing
                // for closing the InputStream
                String contentType = trimHeader(connection.getHeaderField("Content-Type"));
                for (StreamParser<T> responseParser : responseParsers) {
                    for (String acceptType : responseParser.getAcceptType().split(",")) {
                        if (trimHeader(acceptType).equals(contentType)) {
                            return responseParser.parse(cleanup.detach(in));
                        }
                    }
                }
                throw new SQLException(String.format("HTTP request to '%s' failed with unexpected content type: %s", queryEndpoint, contentType));

            } catch (SQLException e) {
                throw e;
            } catch (IOException e) {
                throw new SQLException("I/O exception while parsing HTTP response from server: " + queryEndpoint, e);
            } catch (Exception e) {
                throw new SQLException("Unexpected exception parsing HTTP response from server: " + queryEndpoint, e);
            }
        } catch (SQLException e) {
            throw e;
        } catch (IOException e) {
            throw new SQLException("I/O exception while making HTTP request to server: " + queryEndpoint, e);
        } catch (Exception e) {
            throw new SQLException("Unexpected exception while making HTTP request to server: " + queryEndpoint, e);
        }
    }

    private String trimHeader(String header) {
        return header != null ? header.replaceFirst(";.*", "").trim() : null;
    }

    private String encode(String string) {
        try {
            return URLEncoder.encode(string, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
