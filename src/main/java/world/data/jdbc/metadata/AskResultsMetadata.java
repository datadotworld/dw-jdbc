package world.data.jdbc.metadata;

import org.apache.jena.jdbc.JdbcCompatibility;
import org.apache.jena.jdbc.results.metadata.columns.BooleanColumn;
import org.apache.jena.jdbc.results.metadata.columns.ColumnInfo;
import world.data.jdbc.results.AskResults;
import world.data.jdbc.results.DataWorldResultsSet;

import java.sql.SQLException;

/**
 * Meta data for {@link AskResults}
 * <p>
 * Note that ASK results are something of a special case because they contain
 * only a single column and we know exactly what the type of the column is, with
 * other forms of results we don't have this luxury and so the
 * {@link JdbcCompatibility} levels are used to determine how we report types.
 * </p>
 * 
 */
public class AskResultsMetadata extends DataWorldResultsMetadata {

    /**
     * Constant for the default ASK results column label
     */
    public static final String COLUMN_LABEL_ASK = "ASK";

    /**
     * Constant for the only column index for ASK queries
     */
    public static final int COLUMN_INDEX_ASK = 1;

    private static ColumnInfo[] getColumns() throws SQLException {
        return getColumns(COLUMN_LABEL_ASK);
    }

    private static ColumnInfo[] getColumns(String label) throws SQLException {
        if (label == null)
            label = COLUMN_LABEL_ASK;
        return new ColumnInfo[] { new BooleanColumn(label, columnNoNulls) };
    }

    /**
     * Creates new ASK results metadata
     * 
     * @param results
     *            Results
     * @throws SQLException
     *             Thrown if the metadata cannot be created
     */
    public AskResultsMetadata(DataWorldResultsSet results) throws SQLException {
        super(results, AskResultsMetadata.getColumns());
    }

}
