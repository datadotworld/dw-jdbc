package world.data.jdbc.query;

import org.apache.jena.query.Query;

public class SqlQuery extends Query {
    private final String sql;

    public SqlQuery(final String sql) {
        this.sql = sql;
    }

    @Override
    public String serialize() {
        return sql;
    }
}
