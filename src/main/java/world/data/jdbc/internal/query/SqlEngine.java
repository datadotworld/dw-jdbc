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
import world.data.jdbc.internal.metadata.ColumnFactory;
import world.data.jdbc.internal.metadata.ColumnInfo;
import world.data.jdbc.internal.metadata.ParameterMetaDataImpl;
import world.data.jdbc.internal.metadata.ResultSetMetaDataImpl;
import world.data.jdbc.internal.metadata.SqlDatabaseMetaData;
import world.data.jdbc.internal.results.ResultSetImpl;
import world.data.jdbc.internal.transport.QueryApi;
import world.data.jdbc.internal.transport.Response;
import world.data.jdbc.internal.util.CloseableRef;
import world.data.jdbc.model.Iri;
import world.data.jdbc.model.Node;
import world.data.jdbc.vocab.Xsd;

import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.internal.util.Conditions.check;
import static world.data.jdbc.internal.util.Optionals.mapIfPresent;
import static world.data.jdbc.internal.util.Optionals.or;

public class SqlEngine implements QueryEngine {
    private final QueryApi queryApi;
    private final String catalog;
    private final String schema;

    public SqlEngine(QueryApi queryApi, String catalog, String schema) {
        this.queryApi = requireNonNull(queryApi, "queryApi");
        this.catalog = requireNonNull(catalog, "catalog");
        this.schema = requireNonNull(schema, "schema");
    }

    @Override
    public String getCatalog() {
        return catalog;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getLanguage() {
        return "sql";
    }

    @Override
    public JdbcCompatibility getDefaultCompatibilityLevel() {
        return JdbcCompatibility.HIGH;
    }

    @Override
    public DatabaseMetaData getDatabaseMetaData(DataWorldConnection connection) throws SQLException {
        return new SqlDatabaseMetaData(connection, catalog, schema);
    }

    @Override
    public ParameterMetaData getParameterMetaData(String query) throws SQLException {
        // This is a terrible way to count parameters since it ignores quoted strings, etc.
        // But since we don't really support ParameterMetaData very well it should work for the time being.
        int count = 0;
        for (int i = -1; (i = query.indexOf('?', i + 1)) != -1; ) {
            count++;
        }
        return new ParameterMetaDataImpl(count);
    }

    @Override
    public ResultSet execute(DataWorldStatement statement, String query, Map<String, Node> parameters, Integer timeoutSeconds)
            throws SQLException {
        // Execute the query
        Response response = queryApi.executeQuery(query, parameters, timeoutSeconds);

        // Construct the ResultSet with the results
        try (CloseableRef cleanup = new CloseableRef(response.getCleanup())) {
            check(response.getRows() != null, "SQL response is missing row data");

            ResultSetMetaData metaData = buildResultSetMetaData(response.getColumns());
            ResultSet resultSet = new ResultSetImpl(statement, metaData, response.getRows(), response.getCleanup());

            // The caller becomes responsible for cleaning up
            return cleanup.detach(resultSet);

        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Unexpected exception parsing SQL response from server.", e);
        }
    }

    private ResultSetMetaData buildResultSetMetaData(List<Response.Column> columns) {
        List<ColumnInfo> columnsMetaData = new ArrayList<>();
        for (Response.Column column : columns) {
            columnsMetaData.add(getColumnMetadata(column));
        }
        return new ResultSetMetaDataImpl(columnsMetaData);
    }

    private ColumnInfo getColumnMetadata(Response.Column column) {
        Iri datatype = or(mapIfPresent(column.getDatatypeIri(), Iri::new), Xsd.STRING);
        int nullable = column.isRequired() ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable;
        return ColumnFactory.builder(column.getName(), datatype)
                .nullable(nullable)
                .catalogName(catalog)
                .schemaName(schema)
                .build();
    }

    @Override
    public void checkPositionalParametersSupported() throws SQLException {
        // Nothing to do
    }

    @Override
    public void checkNamedParametersSupported() throws SQLException {
        throw new SQLFeatureNotSupportedException("Named parameters are not supported with SQL.");
    }
}
