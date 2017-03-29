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

import world.data.jdbc.model.Node;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;

/**
 * Model object for a data.world-extended response of type 'application/sparql-results+json' or
 * 'application/rdf+json'.
 */
@lombok.Value
@lombok.Builder(builderClassName = "Builder")
public final class Response {
    private final Boolean booleanResult;
    private final List<Column> columns;
    private final Iterator<Node[]> rows;
    private final AutoCloseable cleanup;

    @lombok.Value
    @lombok.Builder(builderClassName = "Builder")
    public static final class Column {
        private final int index;
        @Nonnull
        private final String name;
        private final String description;
        private final String datatypeIri;
        private final String formatString;
        private final String units;
        private final Double scalingFactor;
        private final boolean required;
    }
}
