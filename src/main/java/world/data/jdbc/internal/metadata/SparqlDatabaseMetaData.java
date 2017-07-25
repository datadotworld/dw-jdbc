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
import world.data.jdbc.Driver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static world.data.jdbc.internal.util.Optionals.nullOrContains;
import static world.data.jdbc.internal.util.Optionals.nullOrEquals;
import static world.data.jdbc.internal.util.Optionals.nullOrMatches;

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
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.COLUMN_COLUMNS);
    }

    @Override
    public String getIdentifierQuoteString() {
        // Not supported in SPARQL so return space per the JDBC javadoc
        return " ";
    }

    @Override
    String getLiteralQuoteString() {
        return "\"";
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
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        if (nullOrEquals(catalog, this.catalog) && nullOrMatches(schemaPattern, this.schema)) {
            rows.add(new Object[]{
                    // TABLE_SCHEM String => schema name
                    schema,
                    // TABLE_CATALOG String => catalog name (may be null)
                    this.catalog,
            });
        }
        return MetaDataSchema.newResultSet(MetaDataSchema.SCHEMA_COLUMNS, rows);
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
    public ResultSet getTableTypes() throws SQLException {
        Object[][] rows = {{"TABLE"}};
        return MetaDataSchema.newResultSet(MetaDataSchema.TABLE_TYPE_COLUMNS, rows);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        if (nullOrEquals(catalog, this.catalog) && nullOrMatches(schemaPattern, this.schema) &&
                nullOrMatches(tableNamePattern, "RDF") && nullOrContains(types, "TABLE")) {
            rows.add(new Object[]{
                    // TABLE_CAT String => table catalog (may be null)
                    this.catalog,
                    // TABLE_SCHEM String => table schema (may be null)
                    schema,
                    // TABLE_NAME String => table name
                    "RDF",
                    // TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE",
                    // "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
                    "TABLE",
                    // REMARKS String => explanatory comment on the table
                    "",
                    // TYPE_CAT String => the types catalog (may be null)
                    null,
                    // TYPE_SCHEM String => the types schema (may be null)
                    null,
                    // TYPE_NAME String => type name (may be null)
                    null,
                    // SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed
                    // table (may be null)
                    null,
                    // REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created.
                    // Values are "SYSTEM", "USER", "DERIVED". (may be null)
                    null,
            });
        }
        return MetaDataSchema.newResultSet(MetaDataSchema.TABLE_COLUMNS, rows);
    }

    @Override
    public String getTimeDateFunctions() {
        return String.join(",", "DAY", "HOURS", "MINUTES", "MONTH", "NOW", "SECONDS", "TIMEZONE", "TZ", "YEAR");
    }

    @Override
    public String getURL() throws SQLException {
        return Driver.SPARQL_PREFIX + catalog + ":" + schema;
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
    public boolean supportsLimitedOuterJoins() {
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
    public boolean supportsUnion() {
        // SPARQL supports UNION
        return true;
    }
}
