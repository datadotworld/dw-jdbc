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
package world.data.jdbc.results;

import fi.iki.elonen.NanoHTTPD;
import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Test;
import world.data.jdbc.NanoHTTPDResource;
import world.data.jdbc.SparqlTest;
import world.data.jdbc.TestConfigSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

public class AskResultsTest {

    private static String lastUri;
    private static String resultResourceName = "ask.json";
    private static String resultMimeType = "application/json";

    @ClassRule
    public static final NanoHTTPDResource proxiedServer = new NanoHTTPDResource(3333) {
        @Override
        protected NanoHTTPD.Response serve(NanoHTTPD.IHTTPSession session) throws Exception {
            final String queryParameterString = session.getQueryParameterString();
            if (queryParameterString != null) {
                lastUri = "http://localhost:3333" + session.getUri() + '?' + queryParameterString;
            } else {
                lastUri = "http://localhost:3333" + session.getUri();
            }
            return newResponse(NanoHTTPD.Response.Status.OK, resultMimeType, IOUtils.toString(SparqlTest.class.getResourceAsStream("/" + resultResourceName)));
        }
    };

    @Test
    public void absolute() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            assertThat(resultSet.absolute(1)).isTrue();
            assertThat(resultSet.getRow()).isEqualTo(1);
            assertThat(resultSet.absolute(-1)).isTrue();
            assertThat(resultSet.getRow()).isEqualTo(1);
            assertThat(resultSet.absolute(0)).isTrue();
            assertThat(resultSet.getRow()).isEqualTo(1);
        }
    }

    @Test
    public void relative() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            assertThat(resultSet.relative(1)).isTrue();
            assertThat(resultSet.getRow()).isEqualTo(1);
            assertThat(resultSet.relative(-1)).isTrue();
            assertThat(resultSet.getRow()).isEqualTo(0);
            assertThat(resultSet.relative(0)).isTrue();
            assertThat(resultSet.getRow()).isEqualTo(0);
            assertThat(resultSet.relative(2)).isTrue();
            assertThat(resultSet.getRow()).isEqualTo(2);
            assertThat(resultSet.relative(-2)).isTrue();
            assertThat(resultSet.getRow()).isEqualTo(0);
        }
    }

    @Test(expected = SQLException.class)
    public void relativeClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.close();
            assertThat(resultSet.relative(1)).isTrue();
        }
    }

    @Test(expected = SQLException.class)
    public void relativeOOBE() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            assertThat(resultSet.relative(3)).isTrue();
        }
    }

    @Test(expected = SQLException.class)
    public void absoluteFail() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            assertThat(resultSet.absolute(3)).isTrue();
        }
    }

    @Test(expected = SQLException.class)
    public void absoluteClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.close();
            assertThat(resultSet.absolute(1)).isTrue();
        }
    }

    @Test(expected = SQLException.class)
    public void absoluteTooFar() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.close();
            assertThat(resultSet.absolute(3)).isFalse();
        }
    }

    @Test
    public void afterLast() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.afterLast();
            assertThat(resultSet.isAfterLast()).isTrue();
            assertThat(resultSet.getRow()).isEqualTo(2);
            assertThat(resultSet.isAfterLast()).isTrue();
        }
    }

    @Test
    public void beforeFirst() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            assertThat(resultSet.isBeforeFirst()).isTrue();
            resultSet.beforeFirst();
            assertThat(resultSet.getRow()).isEqualTo(0);
            assertThat(resultSet.isBeforeFirst()).isTrue();
            resultSet.afterLast();
            assertThat(resultSet.isBeforeFirst()).isFalse();
        }
    }

    @Test
    public void findColumn() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            assertThat(resultSet.findColumn("ASK")).isEqualTo(1);
        }
    }

    @Test(expected = SQLException.class)
    public void findColumnFail() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            assertThat(resultSet.findColumn("BAD_ASK")).isEqualTo(1);
        }
    }

    @Test
    public void getFetchDirection() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            assertThat(resultSet.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
            assertThat(resultSet.getType()).isEqualTo(ResultSet.TYPE_SCROLL_INSENSITIVE);
        }
    }

    @Test
    public void getFetchSize() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            assertThat(resultSet.getFetchSize()).isEqualTo(1);
        }
    }

    @Test(expected = SQLException.class)
    public void isAfterLastClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.close();
            assertThat(resultSet.isAfterLast()).isFalse();
        }
    }

    @Test(expected = SQLException.class)
    public void isBeforeFirstClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.close();
            assertThat(resultSet.isBeforeFirst()).isFalse();
        }
    }

    @Test
    public void isClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            assertThat(resultSet.isClosed()).isFalse();
            resultSet.close();
            assertThat(resultSet.isClosed()).isTrue();
        }
    }

    @Test(expected = SQLException.class)
    public void isFirst() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            assertThat(resultSet.isFirst()).isFalse();
            resultSet.next();
            assertThat(resultSet.isFirst()).isTrue();
            resultSet.close();
            assertThat(resultSet.isFirst()).isTrue();
        }
    }

    @Test(expected = SQLException.class)
    public void isLast() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            assertThat(resultSet.isLast()).isFalse();
            resultSet.next();
            assertThat(resultSet.isLast()).isTrue();
            resultSet.close();
            assertThat(resultSet.isLast()).isTrue();
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setFetchDirectionFail() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.setFetchDirection(ResultSet.FETCH_REVERSE);
        }
    }

    @Test
    public void setFetchDirection() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setFetchSize() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.setFetchSize(3);
        }
    }

    @Test(expected = SQLException.class)
    public void findColumnLabel() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final AskResults resultSet = (AskResults) statement.executeQuery("ask{?s ?p ?o.}")) {
            assertThat(resultSet.findColumnLabel(1)).isEqualTo("ASK");
            assertThat(resultSet.findColumnLabel(0));
        }
    }

    @Test(expected = SQLException.class)
    public void getNode() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final AskResults resultSet = (AskResults) statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.next();
            assertThat(resultSet.getNode("FOO"));
        }
    }

    @Test
    public void getBoolean() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final AskResults resultSet = (AskResults) statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.next();
            assertThat(resultSet.getBoolean("ASK")).isTrue();
        }
    }

    @Test(expected = SQLException.class)
    public void getBooleanClosed() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final AskResults resultSet = (AskResults) statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.close();
            assertThat(resultSet.getBoolean("ASK")).isTrue();
        }
    }

    @Test(expected = SQLException.class)
    public void getBooleanBadRow() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final AskResults resultSet = (AskResults) statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.beforeFirst();
            assertThat(resultSet.getBoolean("ASK")).isTrue();
        }
    }

    @Test(expected = SQLException.class)
    public void getBooleanBaColumn() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final AskResults resultSet = (AskResults) statement.executeQuery("ask{?s ?p ?o.}")) {
            resultSet.next();
            assertThat(resultSet.getBoolean("FOO")).isTrue();
        }
    }

    @org.junit.Test
    public void testAsk() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery("ask{?s ?p ?o.}")) {
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                System.out.print(rsmd.getColumnName(i));
            }
            System.out.println("");
            while (resultSet.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = resultSet.getString(i);
                    System.out.print(columnValue);
                }
                System.out.println("");
            }
        }
        assertThat(lastUri).isEqualTo("http://localhost:3333/sparql/dave/lahman-sabremetrics-dataset?query=ASK%0AWHERE%0A++%7B+%3Fs++%3Fp++%3Fo+%7D%0A");
    }
}