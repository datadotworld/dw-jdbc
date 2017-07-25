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
import world.data.jdbc.internal.types.TypeMap;
import world.data.jdbc.internal.types.TypeMapping;
import world.data.jdbc.model.Iri;
import world.data.jdbc.vocab.Xsd;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static world.data.jdbc.internal.util.Optionals.mapIfPresent;
import static world.data.jdbc.internal.util.Optionals.nullOrContains;
import static world.data.jdbc.internal.util.Optionals.or;

/**
 * Database metadata for Sql connections
 */
public final class SqlDatabaseMetaData extends AbstractDatabaseMetaData {

    /**
     * Creates new connection metadata
     *
     * @param connection Connection
     */
    public SqlDatabaseMetaData(DataWorldConnection connection, String catalog, String schema) {
        super(connection, catalog, schema);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT *" +
                        " FROM TableColumns" +
                        " WHERE owner = COALESCE(?,owner)" +
                        " AND dataset LIKE ?" +
                        " AND tableName LIKE ?" +
                        " AND columnName LIKE ?" +
                        " ORDER BY owner, dataset, tableName, columnIndex")) {
            int index = 0;
            statement.setString(++index, catalog);
            statement.setString(++index, or(schemaPattern, "%"));
            statement.setString(++index, or(tableNamePattern, "%"));
            statement.setString(++index, or(columnNamePattern, "%"));
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String owner = resultSet.getString("owner");
                String dataset = resultSet.getString("dataset");
                String tableName = resultSet.getString("tableName");
                int columnIndex = resultSet.getInt("columnIndex");
                String columnName = resultSet.getString("columnName");
                Iri datatype = mapIfPresent(resultSet.getString("columnDatatype"), Iri::new);
                Boolean nullable = resultSet.getObject("columnNullable", Boolean.class);
                TypeMapping mapping = TypeMap.INSTANCE.getStandardOrCustom(datatype);
                rows.add(new Object[]{
                        // TABLE_CAT String => table catalog (may be null)
                        owner,
                        // TABLE_SCHEM String => table schema (may be null)
                        dataset,
                        // TABLE_NAME String => table name
                        tableName,
                        // COLUMN_NAME String => column name
                        columnName,
                        // DATA_TYPE int => SQL type from java.sql.Types
                        mapping.getJdbcType().getVendorTypeNumber(),
                        // TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
                        mapping.getDatatype(),
                        // COLUMN_SIZE int => column size.
                        mapping.getPrecision(),
                        // BUFFER_LENGTH is not used.
                        null,
                        // DECIMAL_DIGITS int => the number of fractional digits.
                        // Null is returned for data types where DECIMAL_DIGITS is not applicable.
                        mapping.getMaxScale(),
                        // NUM_PREC_RADIX int => Radix (typically either 10 or 2)
                        10,
                        // NULLABLE int => is NULL allowed.
                        // columnNoNulls - might not allow NULL values
                        // columnNullable - definitely allows NULL values
                        // columnNullableUnknown - nullability unknown
                        nullable == null ? columnNullableUnknown : nullable ? columnNullable : columnNoNulls,
                        // REMARKS String => comment describing column (may be null),
                        null,
                        // COLUMN_DEF String => default value for the column, which should
                        // be interpreted as a string when the value is enclosed in single quotes (may be null)
                        null,
                        // SQL_DATA_TYPE int => unused
                        null,
                        // SQL_DATETIME_SUB int => unused
                        null,
                        // CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
                        Xsd.STRING.equals(mapping.getDatatype()) ? Integer.MAX_VALUE : null,
                        // ORDINAL_POSITION int => index of column in table (starting at 1)
                        columnIndex,
                        // IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
                        // YES --- if the parameter can include NULLs
                        // NO --- if the parameter cannot include NULLs
                        // empty string --- if the nullability for the parameter is unknown
                        nullable == null ? "" : nullable ? "YES" : "NO",
                        // SCOPE_CATLOG String => catalog of table that is the scope of a
                        // reference attribute (null if DATA_TYPE isn't REF)
                        null,
                        // SCOPE_SCHEMA String => schema of table that is the scope of a
                        // reference attribute (null if the DATA_TYPE isn't REF)
                        null,
                        // SCOPE_TABLE String => table name that this the scope of a
                        // reference attribure (null if the DATA_TYPE isn't REF)
                        null,
                        // SOURCE_DATA_TYPE short => source type of a distinct type or
                        // user-generated Ref type, SQL type from java.sql.Types (null if
                        // DATA_TYPE isn't DISTINCT or user-generated REF)
                        null,
                        // IS_AUTOINCREMENT String => Indicates whether this column is auto incremented
                        // YES --- if the column is auto incremented
                        // NO --- if the column is not auto incremented
                        // empty string --- if it cannot be determined whether the column is
                        // auto incremented parameter is unknown
                        "NO",
                });
            }
        }
        return MetaDataSchema.newResultSet(MetaDataSchema.COLUMN_COLUMNS, rows);
    }

    @Override
    public String getIdentifierQuoteString() {
        return "`";
    }

    @Override
    String getLiteralQuoteString() {
        return "'";
    }

    @Override
    public String getNumericFunctions() {
        return String.join(",", "ABS", "CEIL", "FLOOR", "ROUND");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT DISTINCT owner, dataset" +
                        " FROM Tables" +
                        " WHERE owner = COALESCE(?,owner)" +
                        " AND dataset LIKE ?" +
                        " ORDER BY owner, dataset")) {
            int index = 0;
            statement.setString(++index, catalog);
            statement.setString(++index, or(schemaPattern, "%"));
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String owner = resultSet.getString("owner");
                String dataset = resultSet.getString("dataset");
                rows.add(new Object[]{
                        // TABLE_SCHEM String => schema name
                        dataset,
                        // TABLE_CATALOG String => catalog name (may be null)
                        owner,
                });
            }
        }
        return MetaDataSchema.newResultSet(MetaDataSchema.SCHEMA_COLUMNS, rows);
    }

    @Override
    public String getSQLKeywords() {
        // TODO Use http://developer.mimer.com/validator/sql-reserved-words.tml
        // as a reference to remove those that also count as SQL Keywords
        return String.join(",", "AND", "AS", "ASC", "BY", "CAST", "DESC", "DISTINCT", "FROM", "FULL", "GROUP",
                "HAVING", "IN", "INNER", "INTERSECT", "JOIN", "LEFT", "LIKE", "LIMIT", "MINUS", "NATURAL", "NOT",
                "NULL", "OFFSET", "ON", "OR", "ORDER", "OUTER", "RIGHT", "SELECT", "UNION", "USING", "WHERE");
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getStringFunctions() {
        return String.join(",", "CONCAT", "LENGTH", "LENGTH", "LOWER", "REGEX", "REPLACE", "SUBSTRING", "UPPER");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        Object[][] rows = {{"TABLE"}};
        return MetaDataSchema.newResultSet(MetaDataSchema.TABLE_TYPE_COLUMNS, rows);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        if (nullOrContains(types, "TABLE")) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT DISTINCT owner, dataset, tableName" +
                            " FROM Tables" +
                            " WHERE owner = COALESCE(?,owner)" +
                            " AND dataset LIKE ?" +
                            " AND tableName LIKE ?" +
                            " ORDER BY owner, dataset, tableName")) {
                int index = 0;
                statement.setString(++index, catalog);
                statement.setString(++index, or(schemaPattern, "%"));
                statement.setString(++index, or(tableNamePattern, "%"));
                ResultSet resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    String owner = resultSet.getString("owner");
                    String dataset = resultSet.getString("dataset");
                    String tableName = resultSet.getString("tableName");
                    rows.add(new Object[]{
                            // TABLE_CAT String => table catalog (may be null)
                            owner,
                            // TABLE_SCHEM String => table schema (may be null)
                            dataset,
                            // TABLE_NAME String => table name
                            tableName,
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
            }
        }
        return MetaDataSchema.newResultSet(MetaDataSchema.TABLE_COLUMNS, rows);
    }

    @Override
    public String getTimeDateFunctions() {
        return String.join(",", "DAY", "HOURS", "MINUTES", "MONTH", "NOW", "SECONDS", "YEAR");
    }

    @Override
    public String getURL() throws SQLException {
        return Driver.SQL_PREFIX + catalog + ":" + schema;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return true;
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
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsUnion() {
        return true;
    }

    @Override
    public boolean supportsUnionAll() {
        // No SPARQL equivalent of UNION ALL
        return true;
    }
}
