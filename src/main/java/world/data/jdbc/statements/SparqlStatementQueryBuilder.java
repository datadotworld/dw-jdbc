package world.data.jdbc.statements;

import org.apache.jena.atlas.web.auth.HttpAuthenticator;
import org.apache.jena.jdbc.statements.metadata.JenaParameterMetadata;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import world.data.jdbc.connections.DataWorldConnection;
import world.data.jdbc.results.AskResults;
import world.data.jdbc.results.SelectResults;
import world.data.jdbc.results.TripleIteratorResults;

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SparqlStatementQueryBuilder implements QueryBuilder {

    @Override
    public Query buildQuery(final String sql) throws SQLException {
        try {
            return  QueryFactory.create(sql);
        } catch (Exception e) {
            throw new SQLException("Not a valid SPARQL query", e);
        }
    }

    @Override
    public ParameterMetaData buildParameterMetadata(final String query) throws SQLException {
        return new JenaParameterMetadata(new ParameterizedSparqlString(query));
    }

    @Override
    public ResultSet buildResults(final DataWorldStatement statement, final Query q, final QueryExecution qe) throws SQLException {
        // Return the appropriate result set type
        if (q.isSelectType()) {
            return new SelectResults(statement, qe, qe.execSelect());
        } else if (q.isAskType()) {
            boolean askRes = qe.execAsk();
            qe.close();
            return new AskResults(statement, askRes);
        } else if (q.isDescribeType()) {
            return  new TripleIteratorResults(statement, qe, qe.execDescribeTriples());
        } else{
            return new TripleIteratorResults(statement, qe, qe.execDescribeTriples());
        }
    }





}
