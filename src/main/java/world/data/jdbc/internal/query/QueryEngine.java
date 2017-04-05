/*
 * dw-jdbc
 * Copyright 2017 data.world, Inc.

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
package world.data.jdbc.internal.query;

import world.data.jdbc.DataWorldConnection;
import world.data.jdbc.DataWorldStatement;
import world.data.jdbc.JdbcCompatibility;
import world.data.jdbc.model.Node;

import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface QueryEngine {

    String getCatalog();

    String getSchema();

    String getLanguage();

    JdbcCompatibility getDefaultCompatibilityLevel();

    DatabaseMetaData getDatabaseMetaData(DataWorldConnection connection) throws SQLException;

    ParameterMetaData getParameterMetaData(String query) throws SQLException;

    ResultSet execute(DataWorldStatement statement, String query, Map<String, Node> parameters, Integer timeoutSeconds)
            throws SQLException;

    void checkPositionalParametersSupported() throws SQLException;

    void checkNamedParametersSupported() throws SQLException;
}
