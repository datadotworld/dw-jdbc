package world.data.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import static org.assertj.core.api.Assertions.assertThat;


public class DataWorldSqlMetadataTest {
    @Test
    public void test() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        final DatabaseMetaData metaData = connection.getMetaData();
        assertThat(metaData.allProceduresAreCallable()).isFalse();
        assertThat(metaData.allTablesAreSelectable()).isTrue();
        assertThat(metaData.autoCommitFailureClosesAllResultSets()).isFalse();
        assertThat(metaData.dataDefinitionCausesTransactionCommit()).isTrue();
        assertThat(metaData.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
        assertThat(metaData.doesMaxRowSizeIncludeBlobs()).isTrue();
        assertThat(metaData.generatedKeyAlwaysReturned()).isFalse();
        assertThat(metaData.getDatabaseMinorVersion()).isEqualTo(0);
        assertThat(metaData.getDatabaseMajorVersion()).isEqualTo(1);
        assertThat(metaData.getDriverName()).isEqualTo("data.world JDBC driver");
        assertThat(metaData.getDriverVersion()).isEqualTo("1.0");
        assertThat(metaData.getDriverMinorVersion()).isEqualTo(0);
        assertThat(metaData.getDriverMajorVersion()).isEqualTo(1);
        assertThat(metaData.getConnection()).isEqualTo(connection);
        assertThat(metaData.getDefaultTransactionIsolation()).isEqualTo(Connection.TRANSACTION_NONE);
        assertThat(metaData.getDatabaseProductName()).isEqualTo("data.world");
        assertThat(metaData.getDatabaseProductVersion()).isEqualTo("1.0");
        assertThat(metaData.getExtraNameCharacters()).isEqualTo("");
        assertThat(metaData.getIdentifierQuoteString()).isEqualTo(" ");
        assertThat(metaData.getJDBCMajorVersion()).isEqualTo(4);
        assertThat(metaData.getJDBCMinorVersion()).isEqualTo(0);
        assertThat(metaData.getMaxBinaryLiteralLength()).isEqualTo(0);
        assertThat(metaData.getMaxCatalogNameLength()).isEqualTo(0);
        assertThat(metaData.getMaxCharLiteralLength()).isEqualTo(0);
        assertThat(metaData.getMaxColumnNameLength()).isEqualTo(0);
        assertThat(metaData.getMaxColumnsInGroupBy()).isEqualTo(0);
        assertThat(metaData.getMaxColumnsInIndex()).isEqualTo(0);
        assertThat(metaData.getMaxColumnsInSelect()).isEqualTo(0);
        assertThat(metaData.getMaxColumnsInTable()).isEqualTo(0);
        assertThat(metaData.getMaxConnections()).isEqualTo(0);
        assertThat(metaData.getMaxCursorNameLength()).isEqualTo(0);
        assertThat(metaData.getMaxColumnsInSelect()).isEqualTo(0);
        assertThat(metaData.getMaxColumnsInGroupBy()).isEqualTo(0);
        assertThat(metaData.getMaxColumnsInOrderBy()).isEqualTo(0);
        assertThat(metaData.getMaxIndexLength()).isEqualTo(0);
        assertThat(metaData.getMaxLogicalLobSize()).isEqualTo(0);
        assertThat(metaData.getMaxProcedureNameLength()).isEqualTo(0);
        assertThat(metaData.getMaxSchemaNameLength()).isEqualTo(0);
        assertThat(metaData.getMaxStatementLength()).isEqualTo(0);
        assertThat(metaData.getMaxStatements()).isEqualTo(0);
        assertThat(metaData.getMaxRowSize()).isEqualTo(0);
        assertThat(metaData.getMaxUserNameLength()).isEqualTo(0);
        assertThat(metaData.getMaxTableNameLength()).isEqualTo(0);
        assertThat(metaData.getMaxTablesInSelect()).isEqualTo(0);
      //  assertThat(metaData.getURL()).isEqualTo(0);
      //  assertThat(metaData.getUserName()).isEqualTo(0);
        assertThat(metaData.getSQLKeywords()).isEqualTo("SELECT,DISTINCT,FROM,JOIN,LEFT,RIGHT,FULL,OUTER,INNER,ON,USING,NATURAL,WHERE,GROUP,BY,ORDER,ASC,DESC,HAVING,LIMIT,OFFSET,UNION,INTERSECT,MINUS,AS,AND,OR,NOT,IN,NULL,LIKE,CAST");
        assertThat(metaData.getSQLStateType()).isEqualTo(1);
        assertThat(metaData.getStringFunctions()).isEqualTo("LENGTH,UPPER,LOWER,CONCAT,LENGTH,SUBSTRING,REPLACE,REGEX");
        assertThat(metaData.getNumericFunctions()).isEqualTo("ABS,CEIL,FLOOR,ROUND");
        assertThat(metaData.getTimeDateFunctions()).isEqualTo("YEAR,MONTH,DAY,HOURS,MINUTES,SECONDS,NOW");
        assertThat(metaData.supportsAlterTableWithAddColumn()).isFalse();
        assertThat(metaData.supportsAlterTableWithDropColumn()).isFalse();
        assertThat(metaData.supportsANSI92EntryLevelSQL()).isFalse();
        assertThat(metaData.supportsANSI92IntermediateSQL()).isFalse();
        assertThat(metaData.supportsANSI92FullSQL()).isFalse();
        assertThat(metaData.supportsBatchUpdates()).isFalse();
        assertThat(metaData.supportsCatalogsInDataManipulation()).isFalse();
        assertThat(metaData.supportsCatalogsInIndexDefinitions()).isFalse();
        assertThat(metaData.supportsCatalogsInProcedureCalls()).isFalse();
        assertThat(metaData.supportsCatalogsInPrivilegeDefinitions()).isFalse();
        assertThat(metaData.supportsCatalogsInTableDefinitions()).isFalse();
        assertThat(metaData.supportsColumnAliasing()).isTrue();
        assertThat(metaData.supportsConvert()).isFalse();
        assertThat(metaData.supportsConvert(2, 3)).isFalse();
        assertThat(metaData.supportsCoreSQLGrammar()).isFalse();
        assertThat(metaData.supportsCorrelatedSubqueries()).isFalse();
        assertThat(metaData.supportsColumnAliasing()).isTrue();
        assertThat(metaData.supportsDifferentTableCorrelationNames()).isFalse();
        assertThat(metaData.supportsExpressionsInOrderBy()).isTrue();
        assertThat(metaData.supportsExtendedSQLGrammar()).isFalse();
        assertThat(metaData.supportsFullOuterJoins()).isTrue();
        assertThat(metaData.supportsGetGeneratedKeys()).isFalse();
        assertThat(metaData.supportsGroupBy()).isTrue();
        assertThat(metaData.supportsGroupByBeyondSelect()).isTrue();
        assertThat(metaData.supportsGroupByUnrelated()).isTrue();
        assertThat(metaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT)).isFalse();
        assertThat(metaData.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT)).isTrue();
        assertThat(metaData.supportsIntegrityEnhancementFacility()).isFalse();
        assertThat(metaData.supportsOuterJoins()).isTrue();
        assertThat(metaData.supportsFullOuterJoins()).isTrue();
        assertThat(metaData.supportsLimitedOuterJoins()).isTrue();
        assertThat(metaData.supportsLikeEscapeClause()).isFalse();
        assertThat(metaData.supportsMinimumSQLGrammar()).isFalse();
        assertThat(metaData.supportsMixedCaseIdentifiers()).isTrue();
        assertThat(metaData.supportsMixedCaseQuotedIdentifiers()).isTrue();
        assertThat(metaData.supportsMultipleOpenResults()).isTrue();
        assertThat(metaData.supportsMultipleResultSets()).isFalse();
        assertThat(metaData.supportsMultipleTransactions()).isTrue();
        assertThat(metaData.supportsNamedParameters()).isFalse();
        assertThat(metaData.supportsNonNullableColumns()).isFalse();
        assertThat(metaData.supportsOpenCursorsAcrossCommit()).isFalse();
        assertThat(metaData.supportsOpenCursorsAcrossRollback()).isFalse();
        assertThat(metaData.supportsOpenStatementsAcrossCommit()).isTrue();
        assertThat(metaData.supportsOpenStatementsAcrossRollback()).isTrue();
        assertThat(metaData.supportsOrderByUnrelated()).isTrue();
        assertThat(metaData.supportsPositionedUpdate()).isFalse();
        assertThat(metaData.supportsPositionedDelete()).isFalse();
        assertThat(metaData.supportsRefCursors()).isFalse();
        assertThat(metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)).isTrue();
        assertThat(metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)).isFalse();
        assertThat(metaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)).isFalse();
        assertThat(metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY)).isTrue();
        assertThat(metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)).isFalse();
        assertThat(metaData.supportsSavepoints()).isFalse();
        assertThat(metaData.supportsSchemasInIndexDefinitions()).isFalse();
        assertThat(metaData.supportsSchemasInDataManipulation()).isFalse();
        assertThat(metaData.supportsSchemasInPrivilegeDefinitions()).isFalse();
        assertThat(metaData.supportsSchemasInProcedureCalls()).isFalse();
        assertThat(metaData.supportsSchemasInTableDefinitions()).isFalse();
        assertThat(metaData.supportsSelectForUpdate()).isFalse();
        assertThat(metaData.supportsStatementPooling()).isFalse();
        assertThat(metaData.supportsStoredFunctionsUsingCallSyntax()).isFalse();
        assertThat(metaData.supportsStoredProcedures()).isFalse();
        assertThat(metaData.supportsSubqueriesInComparisons()).isFalse();
        assertThat(metaData.supportsSubqueriesInExists()).isFalse();
        assertThat(metaData.supportsSubqueriesInIns()).isFalse();
        assertThat(metaData.supportsSubqueriesInQuantifieds()).isFalse();
        assertThat(metaData.supportsTransactions()).isFalse();
        assertThat(metaData.supportsUnion()).isTrue();
        assertThat(metaData.supportsUnionAll()).isTrue();
        assertThat(metaData.nullPlusNonNullIsNull()).isTrue();
        assertThat(metaData.nullsAreSortedAtEnd()).isFalse();
        assertThat(metaData.nullsAreSortedAtStart()).isTrue();
        assertThat(metaData.nullsAreSortedHigh()).isFalse();
        assertThat(metaData.nullsAreSortedLow()).isTrue();
        assertThat(metaData.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isTrue();
        assertThat(metaData.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isTrue();
        assertThat(metaData.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isTrue();
        assertThat(metaData.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isTrue();
        assertThat(metaData.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isTrue();
        assertThat(metaData.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isTrue();
        assertThat(metaData.storesLowerCaseIdentifiers()).isFalse();
        assertThat(metaData.storesLowerCaseQuotedIdentifiers()).isFalse();
        assertThat(metaData.storesUpperCaseIdentifiers()).isFalse();
        assertThat(metaData.storesUpperCaseQuotedIdentifiers()).isFalse();
        assertThat(metaData.storesMixedCaseIdentifiers()).isFalse();
        assertThat(metaData.storesMixedCaseQuotedIdentifiers()).isFalse();
        assertThat(metaData.isCatalogAtStart()).isTrue();
        assertThat(metaData.isReadOnly()).isTrue();
        assertThat(metaData.locatorsUpdateCopy()).isFalse();
        assertThat(metaData.supportsDataDefinitionAndDataManipulationTransactions()).isTrue();
        assertThat(metaData.supportsDataManipulationTransactionsOnly()).isTrue();
        assertThat(metaData.dataDefinitionCausesTransactionCommit()).isTrue();
        assertThat(metaData.supportsTableCorrelationNames()).isFalse();
        assertThat(metaData.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
        assertThat(metaData.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
        assertThat(metaData.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
        assertThat(metaData.dataDefinitionIgnoredInTransactions()).isFalse();
        assertThat(metaData.usesLocalFilePerTable()).isFalse();
        assertThat(metaData.usesLocalFiles()).isFalse();
        assertThat(metaData.getURL()).isNull();
        assertThat(metaData.getUserName()).isNull();
        assertThat(metaData.getProcedureTerm()).isNull();
        assertThat(metaData.getSystemFunctions()).isEmpty();
        assertThat(metaData.getSearchStringEscape()).isEqualTo("\\");
        assertThat(metaData.getCatalogTerm()).isEqualTo("RDF Store");
        assertThat(metaData.getCatalogSeparator()).isEqualTo("");
        assertThat(metaData.getSchemaTerm()).isEqualTo("Dataset");
        assertThat(metaData.getRowIdLifetime()).isEqualTo(RowIdLifetime.ROWID_UNSUPPORTED);
        assertThat(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE)).isTrue();
        assertThat(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED)).isFalse();
        assertThat(metaData.getResultSetHoldability()).isEqualTo(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        assertThat(getResultSetSize(metaData.getTypeInfo())).isEqualTo(21);
        assertThat(getResultSetSize(metaData.getSchemas())).isEqualTo(1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testIsWrapperFor() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        final DatabaseMetaData metaData = connection.getMetaData();
        metaData.isWrapperFor(Class.class);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testUnwrap() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        final DatabaseMetaData metaData = connection.getMetaData();
        metaData.unwrap(Class.class);
    }
    @Test(expected = SQLException.class)
    public void testNullConnection() throws SQLException {
        final DatabaseMetaData metaData = new DataWorldSqlMetadata(null);
        metaData.unwrap(Class.class);
    }
    private int getResultSetSize(final ResultSet resultSet) throws SQLException {
        int count = 0;
        while(resultSet.next()){
            count++;
        }
        return count;
    }

}