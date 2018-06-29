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
import world.data.jdbc.internal.metadata.SparqlDatabaseMetaData;
import world.data.jdbc.internal.results.ResultSetImpl;
import world.data.jdbc.internal.transport.QueryApi;
import world.data.jdbc.internal.transport.Response;
import world.data.jdbc.internal.types.TypeMap;
import world.data.jdbc.internal.util.CloseableRef;
import world.data.jdbc.internal.util.PeekingIterator;
import world.data.jdbc.model.Iri;
import world.data.jdbc.model.Literal;
import world.data.jdbc.model.LiteralFactory;
import world.data.jdbc.model.Node;
import world.data.jdbc.vocab.Rdfs;
import world.data.jdbc.vocab.Xsd;

import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static world.data.jdbc.internal.util.Conditions.check;

public final class SparqlEngine implements QueryEngine {
    private final QueryApi queryApi;
    private final String catalog;
    private final String schema;

    public SparqlEngine(QueryApi queryApi, String catalog, String schema) {
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
        return "sparql";
    }

    @Override
    public JdbcCompatibility getDefaultCompatibilityLevel() {
        // By default, type all columns as String
        return JdbcCompatibility.MEDIUM;
    }

    @Override
    public DatabaseMetaData getDatabaseMetaData(DataWorldConnection connection) throws SQLException {
        return new SparqlDatabaseMetaData(connection, catalog, schema);
    }

    @Override
    public ParameterMetaData getParameterMetaData(String query) throws SQLException {
        // This is a terrible way to count parameters since it ignores quoted strings, bound variables, etc.
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
        Integer maxRowsToReturn = statement.getMaxRows() != 0 ? statement.getMaxRows() : null;

        // Execute the query
        Response response = queryApi.executeQuery(query, parameters, maxRowsToReturn, timeoutSeconds);

        // Construct the ResultSet with the results
        try (CloseableRef cleanup = new CloseableRef(response.getCleanup())) {
            // Special case for ASK queries
            Boolean booleanResult = response.getBooleanResult();
            if (booleanResult != null) {
                return createAskResultSet(statement, booleanResult);
            }

            // Normal SELECT/CONSTRUCT/DESCRIBE response
            check(response.getRows() != null, "SQL response is missing row data");

            List<Response.Column> columns = response.getColumns();
            PeekingIterator<Node[]> rows = new PeekingIterator<>(response.getRows());
            Node[] firstRow = rows.peek();

            JdbcCompatibility level = statement.getJdbcCompatibilityLevel();
            List<ColumnInfo> columnInfos = buildColumnsMetadata(columns, level, firstRow);
            ResultSetMetaData metaData = new ResultSetMetaDataImpl(columnInfos);

            ResultSet resultSet = new ResultSetImpl(statement, metaData, rows, response.getCleanup());

            // The caller becomes responsible for cleaning up
            return cleanup.detach(resultSet);

        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Unexpected exception parsing SPARQL response from server.", e);
        }
    }

    private ResultSet createAskResultSet(DataWorldStatement statement, boolean askResult) throws SQLException {
        ColumnInfo singleColumn = ColumnFactory.builder("ASK", Xsd.BOOLEAN)
                .nullable(ResultSetMetaData.columnNoNulls)
                .build();
        List<Node[]> singleRow = Collections.singletonList(new Node[]{
                LiteralFactory.createBoolean(askResult)
        });
        return new ResultSetImpl(statement, new ResultSetMetaDataImpl(singleColumn), singleRow);
    }

    private List<ColumnInfo> buildColumnsMetadata(List<Response.Column> columns, JdbcCompatibility level, Node[] firstRow) {
        List<ColumnInfo> columnsMetaData = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            Response.Column column = columns.get(i);
            Node sampleValue = firstRow != null ? firstRow[i] : null;
            columnsMetaData.add(buildColumnMetadata(column, level, sampleValue));
        }
        return columnsMetaData;
    }

    private ColumnInfo buildColumnMetadata(Response.Column column, JdbcCompatibility level, Node sampleValue) {
        Iri datatype = pickType(level, sampleValue);
        return ColumnFactory.builder(column.getName(), datatype)
                .catalogName(catalog)
                .schemaName(schema)
                .tableName("RDF")
                .build();
    }

    private Iri pickType(JdbcCompatibility level, Node sampleValue) {
        switch (level) {
            case LOW:
                // Type columns as Types.OTHER with Node as the column class
                return TypeMap.DATATYPE_RAW_NODE;

            case MEDIUM:
                // Type columns as NVARCHAR with String as the column class
                return Xsd.STRING;

            case HIGH:
            default:
                // Detect columns types based on first row of results (if any)
                if (sampleValue == null) {
                    // If the value is null fallback to string
                    return Xsd.STRING;
                } else if (sampleValue instanceof Literal) {
                    return ((Literal) sampleValue).getDatatype();
                } else {
                    // Iri or Blank
                    return Rdfs.RESOURCE;  // ResultSet.getObject() will return String
                }
        }
    }

    @Override
    public void checkPositionalParametersSupported() throws SQLException {
        throw new SQLFeatureNotSupportedException("Positional parameters are not supported with Sparql.");
    }

    @Override
    public void checkNamedParametersSupported() throws SQLException {
        // Nothing to do
    }
}
