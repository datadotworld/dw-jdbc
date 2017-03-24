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

import org.apache.jena.graph.Node;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

import static world.data.jdbc.util.Conditions.check;

public class SqlParameterMetadata implements ParameterMetaData {

    private final int paramCount;

    /**
     * Creates new parameter metadata
     */
    public SqlParameterMetadata(String query) throws SQLException {
        check(query != null, "Parameterized query String cannot be null");
        this.paramCount = countParameters(query);
    }

    private int countParameters(final String query) {
        int count = 0;
        for (int i = 0; i < query.length(); i++) {
            //TODO: needs to handle quotes and comments
            if (query.charAt(i) == '?') {
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
        check(param >= 1 && param <= paramCount, "Parameter Index is out of bounds");
    }

    @Override
    public int getParameterCount() {
        return paramCount;
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
