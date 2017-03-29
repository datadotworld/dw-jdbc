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
import world.data.jdbc.model.Node;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Parses a {@link Response} object in
 * <a href="https://www.w3.org/TR/2013/NOTE-rdf-json-20131107/">application/rdf+json</a> format.
 */
final class RdfParser implements StreamParser<Response> {

    @Override
    public String getAcceptType() {
        return "application/rdf+json";
    }

    @Override
    public Response parse(InputStream in) throws Exception {
        JsonParser parser = ParserUtil.JSON_FACTORY.createParser(in);
        Response.Column[] columns = {
                Response.Column.builder().name("Subject").required(true).build(),
                Response.Column.builder().name("Predicate").required(true).build(),
                Response.Column.builder().name("Object").required(true).build(),
        };
        Iterator<Node[]> triples = new TriplesParser(parser);
        return Response.builder()
                .columns(Arrays.asList(columns))
                .rows(triples)
                .cleanup(parser)
                .build();
    }
}
