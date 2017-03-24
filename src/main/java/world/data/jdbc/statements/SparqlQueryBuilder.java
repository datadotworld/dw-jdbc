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

import org.apache.jena.jdbc.statements.metadata.JenaParameterMetadata;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import world.data.jdbc.results.AskResultSet;
import world.data.jdbc.results.SelectResultSet;
import world.data.jdbc.results.TriplesResultSet;

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SparqlQueryBuilder implements QueryBuilder {

    @Override
    public Query buildQuery(final String sparql) throws SQLException {
        try {
            return QueryFactory.create(sparql);
        } catch (Exception e) {
            throw new SQLException("Not a valid SPARQL query", e);
        }
    }

    @Override
    public ParameterMetaData buildParameterMetadata(final String query) throws SQLException {
        return new JenaParameterMetadata(new ParameterizedSparqlString(query));
    }

    @Override
    public ResultSet buildResults(final Statement statement, final Query q, final QueryExecution qe) throws SQLException {
        // Return the appropriate result set type
        switch (q.getQueryType()) {
            case Query.QueryTypeSelect:
                return new SelectResultSet(statement, qe, qe.execSelect());

            case Query.QueryTypeAsk:
                boolean askRes = qe.execAsk();
                qe.close();
                return new AskResultSet(statement, askRes);

            case Query.QueryTypeDescribe:
                return new TriplesResultSet(statement, qe, qe.execDescribeTriples());

            case Query.QueryTypeConstruct:
                return new TriplesResultSet(statement, qe, qe.execConstructTriples());

            default:
                throw new SQLException("Unknown sparql query type");
        }
    }

}
