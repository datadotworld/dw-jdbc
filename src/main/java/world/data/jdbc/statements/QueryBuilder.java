package world.data.jdbc.statements;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

interface QueryBuilder {
    Query buildQuery(String sql) throws SQLException;

    ParameterMetaData buildParameterMetadata(String query) throws SQLException;

    ResultSet buildResults(DataWorldStatement statement, Query q, QueryExecution qe) throws SQLException;

}
