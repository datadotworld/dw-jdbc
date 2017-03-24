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
package world.data.jdbc.metadata;

import org.apache.jena.jdbc.results.metadata.columns.BooleanColumn;
import org.apache.jena.jdbc.results.metadata.columns.ColumnInfo;
import world.data.jdbc.JdbcCompatibility;
import world.data.jdbc.results.AskResultSet;

import java.sql.SQLException;

/**
 * Meta data for {@link AskResultSet}
 * <p>
 * Note that ASK results are something of a special case because they contain
 * only a single column and we know exactly what the type of the column is, with
 * other forms of results we don't have this luxury and so the
 * {@link JdbcCompatibility} levels are used to determine how we report types.
 * </p>
 */
public class AskResultSetMetadata extends AbstractResultSetMetadata {

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
        if (label == null) {
            label = COLUMN_LABEL_ASK;
        }
        return new ColumnInfo[]{
                new BooleanColumn(label, columnNoNulls)
        };
    }

    /**
     * Creates new ASK results metadata
     *
     * @param results Results
     * @throws SQLException Thrown if the metadata cannot be created
     */
    public AskResultSetMetadata(AskResultSet results) throws SQLException {
        super(results, AskResultSetMetadata.getColumns());
    }

}
