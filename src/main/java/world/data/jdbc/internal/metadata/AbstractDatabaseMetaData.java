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
import world.data.jdbc.internal.util.Versions;
import world.data.jdbc.vocab.Xsd;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.internal.util.Conditions.check;

/**
 * Base class with functionality common to all query languages.
 */
abstract class AbstractDatabaseMetaData implements DatabaseMetaData {

    /** Constant for the term used for catalogs. */
    private static final String CATALOG_TERM = "Account";

    /** Constant for the term used for schemas. */
    private static final String SCHEMA_TERM = "Dataset";

    static final int NO_LIMIT = 0;

    private static final int UNKNOWN_LIMIT = 0;

    protected final DataWorldConnection connection;
    protected final String catalog;
    protected final String schema;

    /**
     * Creates new connection metadata
     *
     * @param connection Connection
     */
    AbstractDatabaseMetaData(DataWorldConnection connection, String catalog, String schema) {
        this.connection = requireNonNull(connection, "connection");
        this.catalog = requireNonNull(catalog, "catalog");
        this.schema = requireNonNull(schema, "schema");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        check(isWrapperFor(iface), "Not a wrapper for the desired interface");
        return iface.cast(this);
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
    public boolean generatedKeyAlwaysReturned() {
        // We don't support returning keys
        return false;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.ATTRIBUTE_COLUMNS);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.BEST_ROW_IDENTIFIER_COLUMNS);
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public String getCatalogTerm() {
        return CATALOG_TERM;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        Object[][] rows = {{catalog}};
        return MetaDataSchema.newResultSet(MetaDataSchema.CATALOG_COLUMNS, rows);
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.CLIENT_INFO_PROPERTY_COLUMNS);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.COLUMN_PRIVILEGE_COLUMNS);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.COLUMN_COLUMNS);
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.CROSS_REFERENCE_COLUMNS);
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
        return Versions.parseVersionNumbers(DataWorldDriver.VERSION)[0];
    }

    @Override
    public int getDriverMinorVersion() {
        return Versions.parseVersionNumbers(DataWorldDriver.VERSION)[1];
    }

    @Override
    public String getDriverName() {
        return "data.world JDBC driver";
    }

    @Override
    public String getDriverVersion() {
        return DataWorldDriver.VERSION;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.EXPORTED_KEY_COLUMNS);
    }

    @Override
    public String getExtraNameCharacters() {
        // Since SPARQL doesn't really have a notion of identifiers like SQL
        // does we return that there are no extra name characters
        return "";
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.FUNCTION_COLUMN_COLUMNS);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.FUNCTION_COLUMNS);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.IMPORTED_KEY_COLUMNS);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.INDEX_INFO_COLUMNS);
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 2;
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        // No limit on RDF term sizes
        return NO_LIMIT;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 31;
    }

    @Override
    public int getMaxCharLiteralLength() {
        // No limit on RDF term sizes
        return NO_LIMIT;
    }

    @Override
    public int getMaxColumnNameLength() {
        // No limit on column name lengths
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
        return 95;
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
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.PRIMARY_KEY_COLUMNS);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.PROCEDURE_COLUMN_COLUMNS);
    }

    @Override
    public String getProcedureTerm() {
        // Not supported
        return null;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.PROCEDURE_COLUMNS);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.PSUEDO_COLUMN_COLUMNS);
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
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
        Object[][] rows = {{schema, catalog}};
        return MetaDataSchema.newResultSet(MetaDataSchema.SCHEMA_COLUMNS, rows);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        if (this.catalog.equals(catalog) && (schemaPattern == null || this.schema.equals(schemaPattern))) {
            return getSchemas();
        } else {
            return MetaDataSchema.newResultSet(MetaDataSchema.SCHEMA_COLUMNS);
        }
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.SUPER_TABLE_COLUMNS);
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.SUPER_TYPE_COLUMNS);
    }

    @Override
    public String getSystemFunctions() {
        // No system functions supported
        return "";
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.TABLE_PRIVILEGE_COLUMNS);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.TABLE_TYPE_COLUMNS);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.TABLE_COLUMNS);
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
        int bytePrecision = Byte.toString(Byte.MAX_VALUE).length();
        int intPrecision = Integer.toString(Integer.MAX_VALUE).length();
        int longPrecision = Long.toString(Long.MAX_VALUE).length();
        Object[][] rows = new Object[][]{
                {Xsd.BOOLEAN, Types.BOOLEAN, 0, null, null, null, typeNullable, false, typeSearchable, false, false, false, null, 0, 0, 0, 0, 0},
                {Xsd.BYTE, Types.TINYINT, bytePrecision, "\"", "\"", null, typeNullable, false, typeSearchable, false, false, false, null, 0, 0, 0, 0, 0},
                {Xsd.DATE, Types.DATE, 0, "\"", "\"", null, typeNullable, false, typeSearchable, false, false, false, null, 0, 0, 0, 0, 0},
                {Xsd.DATETIME, Types.TIMESTAMP, 0, "\"", "\"", null, typeNullable, false, typeSearchable, false, false, false, null, 0, 0, 0, 0, 0},
                {Xsd.DECIMAL, Types.DECIMAL, 16, null, null, null, typeNullable, false, typeSearchable, false, false, false, null, 0, (short) 16, 0, 0, 10},
                {Xsd.DOUBLE, Types.DOUBLE, 16, null, null, null, typeNullable, false, typeSearchable, false, false, false, null, 0, (short) 16, 0, 0, 10},
                {Xsd.FLOAT, Types.FLOAT, 15, "\"", "\"", null, typeNullable, false, typeSearchable, false, false, false, null, 0, (short) 7, 0, 0, 10},
                {Xsd.SHORT, Types.INTEGER, intPrecision, "\"", "\"", null, typeNullable, false, typeSearchable, false, false, false, null, 0, 0, 0, 0, 10},
                {Xsd.INTEGER, Types.BIGINT, longPrecision, null, null, null, typeNullable, false, typeSearchable, false, false, false, null, 0, 0, 0, 0, 10},
                {Xsd.LONG, Types.BIGINT, longPrecision, "\"", "\"", null, typeNullable, false, typeSearchable, false, false, false, null, 0, 0, 0, 0, 10},
                {Xsd.INT, Types.BIGINT, longPrecision, "\"", "\"", null, typeNullable, false, typeSearchable, false, false, false, null, 0, 0, 0, 0, 10},
                {Xsd.NEGATIVEINTEGER, Types.BIGINT, longPrecision, "\"", "\"", null, typeNullable, false, typeSearchable, false, false, false, null, 0, 0, 0, 0, 10},
                {Xsd.NONNEGATIVEINTEGER, Types.BIGINT, longPrecision, "\"", "\"", null, typeNullable, false, typeSearchable, true, false, false, null, 0, 0, 0, 0, 10},
                {Xsd.NONPOSITIVEINTEGER, Types.BIGINT, longPrecision, "\"", "\"", null, typeNullable, false, typeSearchable, false, false, false, null, 0, 0, 0, 0, 10},
                {Xsd.POSITIVEINTEGER, Types.BIGINT, longPrecision, "\"", "\"", null, typeNullable, false, typeSearchable, true, false, false, null, 0, 0, 0, 0, 10},
                {Xsd.UNSIGNEDBYTE, Types.TINYINT, bytePrecision, "\"", "\"", null, typeNullable, false, typeSearchable, true, false, false, null, 0, 0, 0, 0, 10},
                {Xsd.UNSIGNEDINT, Types.BIGINT, longPrecision, "\"", "\"", null, typeNullable, false, typeSearchable, true, false, false, null, 0, 0, 0, 0, 10},
                {Xsd.UNSIGNEDLONG, Types.BIGINT, longPrecision, "\"", "\"", null, typeNullable, false, typeSearchable, true, false, false, null, 0, 0, 0, 0, 10},
                {Xsd.UNSIGNEDSHORT, Types.INTEGER, intPrecision, "\"", "\"", null, typeNullable, false, typeSearchable, true, false, false, null, 0, 0, 0, 0, 10},
                {Xsd.STRING, Types.NVARCHAR, 0, "\"", "\"", null, typeNullable, true, typeSearchable, false, false, false, null, 0, 0, 0, 0, 0},
                {Xsd.TIME, Types.TIME, 0, "\"", "\"", null, typeNullable, false, typeSearchable, false, false, false, null, 0, 0, 0, 0, 0},
        };
        return MetaDataSchema.newResultSet(MetaDataSchema.TYPE_INFO_COLUMNS, rows);
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.UDT_COLUMNS);
    }

    @Override
    public String getUserName() {
        // Not available, login uses an opaque access token
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return MetaDataSchema.newResultSet(MetaDataSchema.VERSION_COLUMNS);
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        // We can't detect inserts that happen while streaming results
        return false;
    }

    @Override
    public boolean isCatalogAtStart() {
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
        // We don't support identifiers in the way that JDBC means so we say false
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        // We don't support identifiers in the way that JDBC means so we say false
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        // We don't support identifiers in the way that JDBC means so we say false
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        // We don't support identifiers in the way that JDBC means so we say false
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        // We don't support identifiers in the way that JDBC means so we say false
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        // We don't support identifiers in the way that JDBC means so we say false
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
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
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
}
