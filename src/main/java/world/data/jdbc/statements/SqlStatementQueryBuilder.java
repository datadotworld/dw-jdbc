package world.data.jdbc.statements;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import world.data.jdbc.query.SqlQuery;
import world.data.jdbc.results.SelectResults;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class SqlStatementQueryBuilder implements QueryBuilder {

    public Query buildQuery(final String sql) throws SQLException {
        return new SqlQuery(sql);
    }

    @Override
    public ParameterMetaData buildParameterMetadata(final String query) throws SQLException {
        return new DataWorldSqlParameterMetadata(query);
    }

    public SelectResults buildResults(final DataWorldStatement statement, final Query q, final QueryExecution qe) throws SQLException {
        return new SelectResults(statement, qe, qe.execSelect());
    }

}
