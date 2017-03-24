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
package world.data.jdbc.statements;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import world.data.jdbc.query.SqlQuery;
import world.data.jdbc.results.SelectResultSet;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class SqlQueryBuilder implements QueryBuilder {

    @Override
    public Query buildQuery(final String sql) throws SQLException {
        return new SqlQuery(sql);
    }

    @Override
    public ParameterMetaData buildParameterMetadata(final String query) throws SQLException {
        return new SqlParameterMetadata(query);
    }

    @Override
    public SelectResultSet buildResults(final Statement statement, final Query q, final QueryExecution qe) throws SQLException {
        return new SelectResultSet(statement, qe, qe.execSelect());
    }

}
