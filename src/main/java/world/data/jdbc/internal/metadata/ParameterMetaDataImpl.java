/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package world.data.jdbc.internal.metadata;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

import static world.data.jdbc.internal.util.Conditions.check;

public class ParameterMetaDataImpl implements ParameterMetaData {
    private final int paramCount;

    public ParameterMetaDataImpl(int paramCount) throws SQLException {
        this.paramCount = paramCount;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        check(isWrapperFor(iface), "Not a wrapper for the desired interface");
        return iface.cast(this);
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        checkParamIndex(param);
        // All parameters are typed as Object
        return Object.class.getCanonicalName();
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
        // Treat all parameters as being typed as Java Objects
        return Types.JAVA_OBJECT;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        checkParamIndex(param);
        // All parameters are typed as Object
        return Object.class.getCanonicalName();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        checkParamIndex(param);
        return parameterNullableUnknown;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        checkParamIndex(param);
        // Return zero since parameters aren't typed as numerics
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        checkParamIndex(param);
        // Return zero since parameters aren't typed as numerics
        return 0;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        checkParamIndex(param);
        // Return false since parameters aren't typed as numerics
        return false;
    }

    private void checkParamIndex(int param) throws SQLException {
        check(param >= 1 && param <= paramCount, "Parameter Index is out of bounds");
    }
}
