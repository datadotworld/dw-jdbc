package world.data.jdbc.metadata;

import org.apache.jena.jdbc.results.metadata.columns.ColumnInfo;
import org.apache.jena.jdbc.results.metadata.columns.SparqlColumnInfo;
import org.apache.jena.jdbc.results.metadata.columns.StringColumn;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.resultset.ResultSetPeekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import world.data.jdbc.JdbcCompatibility;
import world.data.jdbc.results.DataWorldResultsSet;
import world.data.jdbc.results.SelectResults;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.List;

/**
 * Result Set Metadata for {@link SelectResults} instances
 * 
 */
public class SelectResultsMetadata extends DataWorldResultsMetadata {

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectResultsMetadata.class);

    /**
     * Creates new SELECT results metadata
     * 
     * @param results
     *            JDBC result set
     * @param rset
     *            Underlying SPARQL results
     * @throws SQLException
     */
    public SelectResultsMetadata(DataWorldResultsSet results, ResultSetPeekable rset) throws SQLException {
        super(results, makeColumns(results, rset));
    }



    /**
     * Makes column information for SELECT results
     * 
     * @param results
     *            Result Set
     * @param rset
     *            Underlying SPARQL results
     * @return Column information
     * @throws SQLException
     *             Thrown if the column information cannot be created
     */
    private static ColumnInfo[] makeColumns(DataWorldResultsSet results, ResultSetPeekable rset) throws SQLException {
        List<String> vars = rset.getResultVars();
        ColumnInfo[] columns = new ColumnInfo[vars.size()];

        int level = JdbcCompatibility.normalizeLevel(results.getJdbcCompatibilityLevel());
        boolean columnsAsStrings = JdbcCompatibility.shouldTypeColumnsAsString(level);
        boolean columnsDetected = JdbcCompatibility.shouldDetectColumnTypes(level);

        Binding b = null;
        if (columnsDetected) {
            if (rset.hasNext()) {
                b = rset.peekBinding();
            } else {
                // If we were supposed to detect columns but there is no data
                // available then we will just fallback to typing everything as
                // strings
                columnsAsStrings = true;
                columnsDetected = false;
            }
        }

        for (int i = 0; i < columns.length; i++) {
            if (!columnsAsStrings && !columnsDetected) {
                // Low compatibility, report columns as being typed as
                // JAVA_OBJECT with ARQ Node as the column class
                columns[i] = new SparqlColumnInfo(vars.get(i), Types.JAVA_OBJECT, columnNullable);
                LOGGER.info("Low JDBC compatibility, column " + vars.get(i) + " is being typed as Node");
            } else if (columnsAsStrings) {
                // Medium compatibility, report columns as being typed as
                // NVARCHAR with String as the column class
                columns[i] = new StringColumn(vars.get(i), columnNullable);
                LOGGER.info("Medium JDBC compatibility, column " + vars.get(i) + " is being typed as String");
            } else if (columnsDetected) {
                // High compatibility, detect columns types based on first row
                // of results
                columns[i] = JdbcCompatibility.detectColumnType(vars.get(i), b.get(Var.alloc(vars.get(i))), true);
                LOGGER.info("High compatibility, column " + vars.get(i) + " was detected as being of type "
                        + columns[i].getClassName());
            } else {
                throw new SQLFeatureNotSupportedException("Unknown JDBC compatibility level was set");
            }
        }

        return columns;
    }
}
