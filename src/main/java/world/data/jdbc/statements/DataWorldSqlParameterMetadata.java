
package world.data.jdbc.statements;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.jdbc.statements.JenaPreparedStatement;
import org.apache.jena.query.ParameterizedSparqlString;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

public class DataWorldSqlParameterMetadata implements ParameterMetaData {

    private int paramCount;

    /**
     * Creates new parameter metadata
     * @throws SQLException
     */
    public DataWorldSqlParameterMetadata(String query) throws SQLException {
        if (query == null) {
            throw new SQLException("Parameterized query String cannot be null");
        }
        this.paramCount = countParameters(query);
    }

    private int countParameters(final String query) {
        int count = 0;
        for(int i =  0;i<query.length();i++){
            //TODO: needs to handle quotes and comments
            if(query.charAt(i)=='?'){
                count++;
            }
        }
        return count;
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        checkParamIndex(param);
        return Node.class.getCanonicalName();
    }

    private void checkParamIndex(final int param) throws SQLException {
        if (param < 1 || param > this.paramCount) throw new SQLException("Parameter Index is out of bounds");
    }

    @Override
    public int getParameterCount() {
        return this.paramCount;
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        checkParamIndex(param);
        return parameterModeIn;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        checkParamIndex(param);
        return Types.JAVA_OBJECT;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        checkParamIndex(param);
        return Node.class.getCanonicalName();
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        checkParamIndex(param);
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        checkParamIndex(param);
        return 0;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        checkParamIndex(param);
        // Parameters are not nullable
        return parameterNoNulls;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        checkParamIndex(param);
        return false;
    }

}
