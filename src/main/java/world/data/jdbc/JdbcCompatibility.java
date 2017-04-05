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
package world.data.jdbc;

import world.data.jdbc.model.Node;

import java.sql.Types;

/**
 * <p>
 * Class containing constants and helper methods related to JDBC compatibility
 * </p>
 * <h3>Understanding Compatibility Levels</h3>
 * <p>
 * Since JDBC is very SQL centric API by definition shoe-horning SPARQL into it
 * has some caveats and provisos, the aim of this class it to provide some level
 * of configurability of how nice we will try to play with JDBC. We provide the
 * notion of a configurable compatibility level, by definition we use the
 * {@link #MEDIUM} compatibility level, see the documentation on the constants
 * and helper methods to understand exactly what each level means.
 * </p>
 */
public enum JdbcCompatibility {

    /**
     * Constant for low JDBC compatibility level
     * <p>
     * This is the level you should use when you know you are accessing a SPARQL source and are able to cope with
     * the native driver representation of RDF terms natively.
     * </p>
     * <h3>Behavior Specifies</h3>
     * <ul>
     * <li>Column Typing - All result set columns are reported as being as typed as {@link Types#OTHER} and
     * Java type is the {@link Node} type.
     * </li>
     * </ul>
     */
    LOW,

    /**
     * Constant for medium JDBC compatibility level
     * <p>
     * This is the default compatibility level, we will make some effort to be compatible with JDBC but these
     * efforts will not be perfect.
     * </p>
     * <h3>Behavior Specifics</h3>
     * <ul>
     * <li>Column Typing - All result set columns are reported as being as typed as {@link Types#NVARCHAR} and
     * Java type is {@link String}.
     * </li>
     * </ul>
     */
    MEDIUM,

    /**
     * Constant for high JDBC compatibility level
     * <p>
     * This is the highest compatibility level, we will do our best to be compatible with JDBC however these
     * efforts may still not be perfect.
     * </p>
     * <h3>Behavior Specifics</h3>
     * <ul>
     * <li>Column Typing - For SPARQL, result set columns are typed by inspecting the first row of the data and,
     * for SQL, result set columns are typed based on the column metadata returned by the server, so native JDBC
     * types like {@link Types#INTEGER} and so forth may be reported depending on the query.
     * </li>
     * </ul>
     */
    HIGH,
}
