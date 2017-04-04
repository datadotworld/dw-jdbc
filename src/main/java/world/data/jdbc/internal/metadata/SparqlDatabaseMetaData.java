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
package world.data.jdbc.internal.metadata;

import world.data.jdbc.DataWorldConnection;
import world.data.jdbc.DataWorldDriver;

import java.sql.SQLException;

/**
 * Database metadata for Sparql connections
 */
public final class SparqlDatabaseMetaData extends AbstractDatabaseMetaData {

    /**
     * Creates new connection metadata
     *
     * @param connection Connection
     */
    public SparqlDatabaseMetaData(DataWorldConnection connection, String catalog, String schema) {
        super(connection, catalog, schema);
    }

    @Override
    public String getIdentifierQuoteString() {
        // Not supported in SPARQL so return space per the JDBC javadoc
        return " ";
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 3;  // Subject, Predicate, Object (data.world doesn't support quads)
    }

    @Override
    public String getNumericFunctions() {
        return String.join(",", "ABS", "CEIL", "FLOOR", "RAND", "ROUND");
    }

    @Override
    public String getSQLKeywords() {
        return String.join(",", "ABS", "ADD", "ALL", "AS", "ASC", "ASK", "AVG", "BASE", "BIND", "BNODE", "BOUND",
                "BY", "CEIL", "CLEAR", "COALESCE", "CONCAT", "CONSTRUCT", "CONTAINS", "COPY", "COUNT", "CREATE",
                "DATATYPE", "DAY", "DEFAULT", "DELETEDATA", "DELETEWHERE", "DESC", "DESCRIBE", "DISTINCT", "DROP",
                "ENCODE_FOR_URI", "EXISTS", "FILTER", "FLOOR", "FROM", "GRAPH", "GROUP", "GROUP_CONCAT", "HAVING",
                "HOURS", "IF", "IN", "INSERT", "INSERTDATA", "INTO", "IRI", "ISBLANK", "ISIRI", "ISURI", "LANG",
                "LANGMATCHES", "LCASE", "LIMIT", "LOAD", "MAX", "MD5", "MIN", "MINUS", "MINUTES", "MONTH", "MOVE",
                "NAMED", "NOTEXISTS", "NOTIN", "NOW", "OFFSET", "OPTIONAL", "ORDER", "PREFIX", "RAND", "REDUCED",
                "REGEX", "REPLACE", "ROUND", "SAMETERM", "SAMPLE", "SECONDS", "SELECT", "SEPARATOR", "SERVICE",
                "SHA1", "SHA256", "SHA384", "SHA512", "SILENT", "STR", "STRAFTER", "STRBEFORE", "STRDT", "STRENDS",
                "STRLANG", "STRLEN", "STRSTARTS", "STRUUID", "SUBSTR", "SUM", "TIMEZONE", "TZ", "UCASE", "UNDEF",
                "UNION", "URI", "USING", "UUID", "VALUES", "WHERE", "WITH", "YEAR", "a", "false", "true");
    }

    @Override
    public String getSearchStringEscape() {
        // Does not apply to SPARQL
        return "";
    }

    @Override
    public String getStringFunctions() {
        return String.join(",", "CONCAT", "CONTAINS", "ENCODE_FOR_URI", "LANG", "LANGMATCHES", "LCASE", "REGEX",
                "REPLACE", "STR", "STRAFTER", "STRBEFORE", "STRENDS", "STRLEN", "STRSTARTS", "SUBSTR", "UCASE");
    }

    @Override
    public String getTimeDateFunctions() {
        return String.join(",", "DAY", "HOURS", "MINUTES", "MONTH", "NOW", "SECONDS", "TIMEZONE", "TZ", "YEAR");
    }

    @Override
    public String getURL() throws SQLException {
        return DataWorldDriver.SPARQL_PREFIX + catalog + ":" + schema;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return true;  // Via CallableStatement
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        // SPARQL allows ORDER BY on a column that you don't SELECT
        return true;
    }

    @Override
    public boolean supportsOuterJoins() {
        // SPARQL supports all kinds of joins
        return true;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return true;  // Means CallableStatement is supported, required for named parameters
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        // SPARQL does allow sub-queries in EXISTS though strictly speaking our
        // EXISTS has no relation to the SQL equivalent
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        // Can't use subqueries in this way in SPARQL
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        // I have no idea what this mean so assume we can't use sub-queries this
        // way in SPARQL
        return false;
    }

    @Override
    public boolean supportsUnion() {
        // SPARQL supports UNION
        return true;
    }

    @Override
    public boolean supportsUnionAll() {
        // No SPARQL equivalent of UNION ALL
        return false;
    }
}
