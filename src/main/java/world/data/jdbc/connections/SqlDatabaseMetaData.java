/*
* dw-jdbc
* Copyright 2016 data.world, Inc.

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
package world.data.jdbc.connections;

import org.apache.jena.jdbc.metadata.MetadataSchema;
import org.apache.jena.jdbc.metadata.results.MetaResultSet;
import org.apache.jena.vocabulary.XSD;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

import static world.data.jdbc.util.Conditions.check;

/**
 * Database metadata for Sql connections
 */
public class SqlDatabaseMetaData implements DatabaseMetaData {

    /**
     * Constant for the term used for catalogues
     */
    private static final String CATALOG_TERM = "RDF Store";

    /**
     * Constant for the term used for schemas
     */
    private static final String SCHEMA_TERM = "Dataset";

    /**
     * Constant for the default catalog which is the only catalog we report as
     * existing by default
     */
    private static final String DEFAULT_CATALOG = "RDF";

    /**
     * Constant for the default schema which is the only schema we report as
     * existing by default
     */
    private static final String DEFAULT_SCHEMA = "Dataset";

    private static final int NO_LIMIT = 0;

    private static final int UNKNOWN_LIMIT = 0;

    /**
     * Constants for SQL Keywords
     */
    private static final String[] SQL_KEYWORDS = new String[]{"SELECT", "DISTINCT", "FROM", "JOIN", "LEFT", "RIGHT"
            , "FULL", "OUTER", "INNER", "ON", "USING", "NATURAL", "WHERE", "GROUP", "BY", "ORDER", "ASC", "DESC"
            , "HAVING", "LIMIT", "OFFSET", "UNION", "INTERSECT", "MINUS", "AS", "AND", "OR", "NOT", "IN", "NULL", "LIKE"
            , "CAST"};

    /**
     * Constants for SQL numeric functions
     */
    private static final String[] SQL_NUMERIC_FUNCTIONS = new String[]{"ABS", "CEIL", "FLOOR", "ROUND"};

    /**
     * Constants for SQL string functions
     */
    private static final String[] SQL_STR_FUNCTIONS = new String[]{"LENGTH", "UPPER", "LOWER",
            "CONCAT", "LENGTH", "SUBSTRING", "REPLACE", "REGEX"};

    /**
     * Constants for SQL string functions
     */
    private static final String[] SQL_DATETIME_FUNCTIONS = new String[]{"YEAR", "MONTH", "DAY", "HOURS", "MINUTES",
            "SECONDS", "NOW"};
    private DataWorldConnection connection;

    /**
     * Creates new connection metadata
     *
     * @param connection Connection
     */
    public SqlDatabaseMetaData(DataWorldConnection connection) throws SQLException {
        check(connection != null, "Connection cannot be null");
        this.connection = connection;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return true;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        // There is no max row size in RDF/SPARQL
        return true;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return new MetaResultSet(MetadataSchema.getAttributeColumns());
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return new MetaResultSet(MetadataSchema.getBestRowIdentifierColumns());
    }

    @Override
    public String getCatalogSeparator() {
        return "";
    }

    @Override
    public String getCatalogTerm() {
        return CATALOG_TERM;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return new MetaResultSet(MetadataSchema.getCatalogsColumns(), new Object[][]{{DEFAULT_CATALOG}});
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return new MetaResultSet(MetadataSchema.getClientInfoPropertyColumns());
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return new MetaResultSet(MetadataSchema.getColumnPrivilegeColumns());
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return new MetaResultSet(MetadataSchema.getColumnColumns());
    }

    @Override
    public final Connection getConnection() {
        return connection;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return new MetaResultSet(MetadataSchema.getCrossReferenceColumns());
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int isolationLevel) {
        return isolationLevel == Connection.TRANSACTION_NONE;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return 1;
    }

    @Override
    public int getDatabaseMinorVersion() {
        return 0;
    }

    @Override
    public String getDatabaseProductName() {
        return "data.world";
    }

    @Override
    public String getDatabaseProductVersion() {
        return "1.0";
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public int getDriverMajorVersion() {
        return 1;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public String getDriverName() {
        return "data.world JDBC driver";
    }

    @Override
    public String getDriverVersion() {
        return "1.0";
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return new MetaResultSet(MetadataSchema.getExportedKeyColumns());
    }

    @Override
    public String getExtraNameCharacters() {
        // Since SPARQL doesn't really have a notion of identifiers like SQL
        // does we return that there are no extra name characters
        return "";
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return new MetaResultSet(MetadataSchema.getFunctionColumnColumns());
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return new MetaResultSet(MetadataSchema.getFunctionColumns());
    }

    @Override
    public String getIdentifierQuoteString() {
        // Not supported in SPARQL so return space per the JDBC javadoc
        return " ";
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return new MetaResultSet(MetadataSchema.getImportedKeyColumns());
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return new MetaResultSet(MetadataSchema.getIndexInfoColumns());
    }

    @Override
    public final int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public final int getJDBCMinorVersion() {
        return 0;
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        // No limit on RDF term sizes
        return NO_LIMIT;
    }

    @Override
    public int getMaxCatalogNameLength() {
        // No limit on catalog name lengths because we don't
        // really support catalogs
        return NO_LIMIT;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxColumnNameLength() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxColumnsInTable() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxConnections() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxCursorNameLength() {
        return UNKNOWN_LIMIT;
    }

    @Override
    public int getMaxIndexLength() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return UNKNOWN_LIMIT;
    }

    @Override
    public int getMaxRowSize() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxStatementLength() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxStatements() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxTableNameLength() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxTablesInSelect() {
        return NO_LIMIT;
    }

    @Override
    public int getMaxUserNameLength() {
        return UNKNOWN_LIMIT;
    }

    @Override
    public String getNumericFunctions() {
        return String.join(",", SQL_NUMERIC_FUNCTIONS);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return new MetaResultSet(MetadataSchema.getPrimaryKeyColumns());
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return new MetaResultSet(MetadataSchema.getProcedureColumnColumns());
    }

    @Override
    public String getProcedureTerm() {
        // Not supported
        return null;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return new MetaResultSet(MetadataSchema.getProcedureColumns());
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return new MetaResultSet(MetadataSchema.getPsuedoColumnColumns());
    }

    @Override
    public int getResultSetHoldability() {
        return DataWorldConnection.DEFAULT_HOLDABILITY;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        // Not supported
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public String getSQLKeywords() {
        // TODO Use http://developer.mimer.com/validator/sql-reserved-words.tml
        // as a reference to remove those that also count as SQL Keywords
        return String.join(",", SQL_KEYWORDS);
    }

    @Override
    public int getSQLStateType() {
        return sqlStateXOpen;
    }

    @Override
    public String getSchemaTerm() {
        return SCHEMA_TERM;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return new MetaResultSet(MetadataSchema.getSchemaColumns(), new Object[][]{{DEFAULT_SCHEMA, DEFAULT_CATALOG}});
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        if (DEFAULT_CATALOG.equals(catalog)) {
            if (schemaPattern == null || DEFAULT_SCHEMA.equals(schemaPattern)) {
                return getSchemas();
            } else {
                return new MetaResultSet(MetadataSchema.getSchemaColumns());
            }
        } else {
            return new MetaResultSet(MetadataSchema.getSchemaColumns());
        }
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getStringFunctions() {
        return String.join(",", SQL_STR_FUNCTIONS);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new MetaResultSet(MetadataSchema.getSuperTableColumns());
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return new MetaResultSet(MetadataSchema.getSuperTypeColumns());
    }

    @Override
    public String getSystemFunctions() {
        // No system functions supported
        return "";
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new MetaResultSet(MetadataSchema.getTablePrivilegeColumns());
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return new MetaResultSet(MetadataSchema.getTableTypeColumns());
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return new MetaResultSet(MetadataSchema.getTableColumns());
    }

    @Override
    public String getTimeDateFunctions() {
        return String.join(",", SQL_DATETIME_FUNCTIONS);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        // TYPE_NAME String => Type name
        // DATA_TYPE int => SQL data type from java.sql.Types
        // PRECISION int => maximum precision
        // LITERAL_PREFIX String => prefix used to quote a literal (may be null)
        // LITERAL_SUFFIX String => suffix used to quote a literal (may be null)
        // CREATE_PARAMS String => parameters used in creating the type (may be
        // null)
        // NULLABLE short => can you use NULL for this type.
        // typeNoNulls - does not allow NULL values
        // typeNullable - allows NULL values
        // typeNullableUnknown - nullability unknown
        // CASE_SENSITIVE boolean=> is it case sensitive.
        // SEARCHABLE short => can you use "WHERE" based on this type:
        // typePredNone - No support
        // typePredChar - Only supported with WHERE .. LIKE
        // typePredBasic - Supported except for WHERE .. LIKE
        // typeSearchable - Supported for all WHERE ..
        // UNSIGNED_ATTRIBUTE boolean => is it unsigned.
        // FIXED_PREC_SCALE boolean => can it be a money value.
        // AUTO_INCREMENT boolean => can it be used for an auto-increment value.
        // LOCAL_TYPE_NAME String => localized version of type name (may be
        // null)
        // MINIMUM_SCALE short => minimum scale supported
        // MAXIMUM_SCALE short => maximum scale supported
        // SQL_DATA_TYPE int => unused
        // SQL_DATETIME_SUB int => unused
        // NUM_PREC_RADIX int => usually 2 or 10

        // Report types we can marshal appropriately
        return new MetaResultSet(MetadataSchema.getTypeInfoColumns(), new Object[][]{
                {XSD.xboolean.toString(), Types.BOOLEAN, 0, null, null, null, (short) typeNullable, false,
                        (short) typeSearchable, false, false, false, null, (short) 0, (short) 0, 0, 0, 0},
                {XSD.xbyte.toString(), Types.TINYINT, Byte.toString(Byte.MAX_VALUE).length(), "\"", "\"", null,
                        (short) typeNullable, false, (short) typeSearchable, false, false, false, null, (short) 0, (short) 0, 0,
                        0, 0},
                {XSD.date.toString(), Types.DATE, 0, "\"", "\"", null, (short) typeNullable, false, (short) typeSearchable,
                        false, false, false, null, (short) 0, (short) 0, 0, 0, 0},
                {XSD.dateTime.toString(), Types.DATE, 0, "\"", "\"", null, (short) typeNullable, false, (short) typeSearchable,
                        false, false, false, null, (short) 0, (short) 0, 0, 0, 0},
                {XSD.decimal.toString(), Types.DECIMAL, 16, null, null, null, (short) typeNullable, false,
                        (short) typeSearchable, false, false, false, null, (short) 0, (short) 16, 0, 0, 10},
                {XSD.xdouble.toString(), Types.DOUBLE, 16, null, null, null, (short) typeNullable, false,
                        (short) typeSearchable, false, false, false, null, (short) 0, (short) 16, 0, 0, 10},
                {XSD.xfloat.toString(), Types.FLOAT, 15, "\"", "\"", null, (short) typeNullable, false, (short) typeSearchable,
                        false, false, false, null, (short) 0, (short) 7, 0, 0, 10},
                {XSD.xshort.toString(), Types.INTEGER, Integer.toString(Integer.MAX_VALUE).length(), "\"", "\"", null,
                        (short) typeNullable, false, (short) typeSearchable, false, false, false, null, (short) 0, (short) 0, 0,
                        0, 10},
                {XSD.integer.toString(), Types.BIGINT, Long.toString(Long.MAX_VALUE).length(), null, null, null,
                        (short) typeNullable, false, (short) typeSearchable, false, false, false, null, (short) 0, (short) 0, 0,
                        0, 10},
                {XSD.xlong.toString(), Types.BIGINT, Long.toString(Long.MAX_VALUE).length(), "\"", "\"", null,
                        (short) typeNullable, false, (short) typeSearchable, false, false, false, null, (short) 0, (short) 0, 0,
                        0, 10},
                {XSD.xint.toString(), Types.BIGINT, Long.toString(Long.MAX_VALUE).length(), "\"", "\"", null,
                        (short) typeNullable, false, (short) typeSearchable, false, false, false, null, (short) 0, (short) 0, 0,
                        0, 10},
                {XSD.negativeInteger.toString(), Types.BIGINT, Long.toString(Long.MAX_VALUE).length(), "\"", "\"", null,
                        (short) typeNullable, false, (short) typeSearchable, false, false, false, null, (short) 0, (short) 0, 0,
                        0, 10},
                {XSD.nonNegativeInteger.toString(), Types.BIGINT, Long.toString(Long.MAX_VALUE).length(), "\"", "\"", null,
                        (short) typeNullable, false, (short) typeSearchable, true, false, false, null, (short) 0, (short) 0, 0,
                        0, 10},
                {XSD.nonPositiveInteger.toString(), Types.BIGINT, Long.toString(Long.MAX_VALUE).length(), "\"", "\"", null,
                        (short) typeNullable, false, (short) typeSearchable, false, false, false, null, (short) 0, (short) 0, 0,
                        0, 10},
                {XSD.positiveInteger.toString(), Types.BIGINT, Long.toString(Long.MAX_VALUE).length(), "\"", "\"", null,
                        (short) typeNullable, false, (short) typeSearchable, true, false, false, null, (short) 0, (short) 0, 0,
                        0, 10},
                {XSD.unsignedByte.toString(), Types.TINYINT, Byte.toString(Byte.MAX_VALUE).length(), "\"", "\"", null,
                        (short) typeNullable, false, (short) typeSearchable, true, false, false, null, (short) 0, (short) 0, 0,
                        0, 10},
                {XSD.unsignedInt.toString(), Types.BIGINT, Long.toString(Long.MAX_VALUE).length(), "\"", "\"", null,
                        (short) typeNullable, false, (short) typeSearchable, true, false, false, null, (short) 0, (short) 0, 0,
                        0, 10},
                {XSD.unsignedLong.toString(), Types.BIGINT, Long.toString(Long.MAX_VALUE).length(), "\"", "\"", null,
                        (short) typeNullable, false, (short) typeSearchable, true, false, false, null, (short) 0, (short) 0, 0,
                        0, 10},
                {XSD.unsignedShort.toString(), Types.INTEGER, Integer.toString(Integer.MAX_VALUE).length(), "\"", "\"", null,
                        (short) typeNullable, false, (short) typeSearchable, true, false, false, null, (short) 0, (short) 0, 0,
                        0, 10},
                {XSD.xstring.toString(), Types.NVARCHAR, 0, "\"", "\"", null, (short) typeNullable, true,
                        (short) typeSearchable, false, false, false, null, (short) 0, (short) 0, 0, 0, 0},
                {XSD.time.toString(), Types.TIME, 0, "\"", "\"", null, (short) typeNullable, false, (short) typeSearchable,
                        false, false, false, null, (short) 0, (short) 0, 0, 0, 0},});
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return new MetaResultSet(MetadataSchema.getUdtColumns());
    }

    @Override
    public String getURL() {
        // Underlying database is unknown
        return null;
    }

    @Override
    public String getUserName() {
        // No authentication used by default
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return new MetaResultSet(MetadataSchema.getVersionColumns());
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        // We can't detect inserts that happen while streaming results
        return false;
    }

    @Override
    public boolean isCatalogAtStart() {
        // We don't really support catalogs so we'll say yes
        return true;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return connection.isReadOnly();
    }

    @Override
    public boolean locatorsUpdateCopy() {
        // SPARQL doesn't support the LOB types so return false
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        // Concatenating nulls (i.e. unbound/type error) in SPARQL results
        // leads to nulls
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        // SPARQL sort order puts nulls (i.e. unbound) first
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        // SPARQL sort order puts nulls (i.e. unbound) first
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        // SPARQL sort order puts nulls (i.e. unbound) first
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        // SPARQL sort order puts nulls (i.e. unbound) first
        return true;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        // Since results are streamed it may be possible to see deletes from
        // others depending on the underlying implementation
        return true;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        // Since results are streamed it may be possible to see inserts from
        // others depending on the underlying implementation
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        // Since results are streamed it may be possible to see updates from
        // others depending on the underlying implementation
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        // Since results are streamed it may be possible to see deletes from
        // ourselves depending on the underlying implementation
        return true;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        // Since results are streamed it may be possible to see inserts from
        // ourselves depending on the underlying implementation
        return true;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        // Since results are streamed it may be possible to see deletes from
        // others depending on the underlying implementation
        return true;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        // We don't support identifiers in the way that JDBC means so we say
        // false
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        // We don't support identifiers in the way that JDBC means so we say
        // false
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        // We don't support identifiers in the way that JDBC means so we say
        // false
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        // We don't support identifiers in the way that JDBC means so we say
        // false
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        // We don't support identifiers in the way that JDBC means so we say
        // false
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        // We don't support identifiers in the way that JDBC means so we say
        // false
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    @Override
    public boolean supportsConvert() {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return true;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
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
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return true;
    }

    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        // Statements remain open across commits
        return true;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        // Statements remain open across rollbacks
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
    public boolean supportsPositionedDelete() {
        // We don't support deleting from result set
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        // We don't support updating from result set
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        // We only support read-only result sets
        return concurrency == ResultSet.CONCUR_READ_ONLY
                && supportsResultSetType(type);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        // We don't really support tables
        return false;
    }

    @Override
    public boolean supportsTransactions() {
        // Currently transactions are not supported
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
        return true;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        // Updates are never detectable
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        // We don't support returning keys
        return false;
    }

}
