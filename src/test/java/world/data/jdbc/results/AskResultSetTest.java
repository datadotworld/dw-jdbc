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
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import world.data.jdbc.statements.Statement;
import world.data.jdbc.testing.NanoHTTPDHandler;
import world.data.jdbc.testing.NanoHTTPDResource;
import world.data.jdbc.testing.SparqlHelper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static fi.iki.elonen.NanoHTTPD.Method.GET;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;
import static world.data.jdbc.testing.MoreAssertions.assertSQLFeatureNotSupported;

public class AskResultSetTest {

    private static NanoHTTPDHandler lastBackendRequest;
    private static final String resultResourceName = "/ask.json";
    private static final String resultMimeType = "application/json";

    @ClassRule
    public static final NanoHTTPDResource proxiedServer = new NanoHTTPDResource(3333) {
        @Override
        protected NanoHTTPD.Response serve(NanoHTTPD.IHTTPSession session) throws Exception {
            NanoHTTPDHandler.invoke(session, lastBackendRequest);
            String body = IOUtils.toString(getClass().getResourceAsStream(resultResourceName), UTF_8);
            return newResponse(NanoHTTPD.Response.Status.OK, resultMimeType, body);
        }
    };

    @Rule
    public final SparqlHelper sparql = new SparqlHelper();

    @Before
    public void setup() {
        lastBackendRequest = mock(NanoHTTPDHandler.class);
    }

    private ResultSet sampleResultSet() throws SQLException {
        Statement statement = sparql.createStatement(sparql.connect());
        return sparql.executeQuery(statement, "ask{?s ?p ?o.}");
    }

    @Test
    public void absolute() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.absolute(1)).isTrue();
        assertThat(resultSet.getRow()).isEqualTo(1);
        assertThat(resultSet.absolute(-1)).isTrue();
        assertThat(resultSet.getRow()).isEqualTo(1);
        assertThat(resultSet.absolute(0)).isTrue();
        assertThat(resultSet.getRow()).isEqualTo(1);
    }

    @Test
    public void relative() throws Exception {
        ResultSet resultSet = sampleResultSet();
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

    @Test
    public void relativeOOBE() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertSQLException(() -> resultSet.relative(3));
    }

    @Test
    public void absoluteFail() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertSQLException(() -> resultSet.absolute(3));
    }

    @Test
    public void afterLast() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.afterLast();
        assertThat(resultSet.isAfterLast()).isTrue();
        assertThat(resultSet.getRow()).isEqualTo(2);
        assertThat(resultSet.isAfterLast()).isTrue();
    }

    @Test
    public void beforeFirst() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.isBeforeFirst()).isTrue();
        resultSet.beforeFirst();
        assertThat(resultSet.getRow()).isEqualTo(0);
        assertThat(resultSet.isBeforeFirst()).isTrue();
        resultSet.afterLast();
        assertThat(resultSet.isBeforeFirst()).isFalse();
    }

    @Test
    public void findColumn() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.findColumn("ASK")).isEqualTo(1);
    }

    @Test
    public void findColumnFail() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertSQLException(() -> resultSet.findColumn("BAD_ASK"));
    }

    @Test
    public void getFetchDirection() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
        assertThat(resultSet.getType()).isEqualTo(ResultSet.TYPE_SCROLL_INSENSITIVE);
    }

    @Test
    public void getFetchSize() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.getFetchSize()).isEqualTo(1);
    }

    @Test
    public void isClosed() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.isClosed()).isFalse();
        resultSet.close();
        assertThat(resultSet.isClosed()).isTrue();
    }

    @Test
    public void isFirst() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.isFirst()).isFalse();
        resultSet.next();
        assertThat(resultSet.isFirst()).isTrue();
    }

    @Test
    public void isLast() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.isLast()).isFalse();
        resultSet.next();
        assertThat(resultSet.isLast()).isTrue();
    }

    @Test
    public void setFetchDirection() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
    }

    @Test
    public void findColumnLabel() throws Exception {
        AskResultSet resultSet = (AskResultSet) sampleResultSet();
        assertThat(resultSet.findColumnLabel(1)).isEqualTo("ASK");
        assertSQLException(() -> resultSet.findColumnLabel(0));
    }

    @Test
    public void getNode() throws Exception {
        AskResultSet resultSet = (AskResultSet) sampleResultSet();
        resultSet.next();
        assertSQLException(() -> resultSet.getNode("FOO"));
    }

    @Test
    public void getBoolean() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getBoolean("ASK")).isTrue();
    }

    @Test
    public void getBooleanBadRow() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.beforeFirst();
        assertSQLException(() -> resultSet.getBoolean("ASK"));
    }

    @Test
    public void getBooleanBadColumn() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertSQLException(() -> resultSet.getBoolean("FOO"));
    }

    @Test
    public void testAsk() throws Exception {
        ResultSet resultSet = sampleResultSet();
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) {
                System.out.print(",  ");
            }
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println("");
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) {
                    System.out.print(",  ");
                }
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue);
            }
            System.out.println("");
        }
        verify(lastBackendRequest).handle(GET, "/sparql/dave/lahman-sabremetrics-dataset", "query=ASK%0AWHERE%0A++%7B+%3Fs++%3Fp++%3Fo+%7D%0A");
    }

    @Test
    public void testAllClosed() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.close();
        assertSQLException(() -> resultSet.absolute(1));
        assertSQLException(() -> resultSet.absolute(3));
        assertSQLException(() -> resultSet.getBoolean("ASK"));
        assertSQLException(resultSet::isAfterLast);
        assertSQLException(resultSet::isBeforeFirst);
        assertSQLException(resultSet::isFirst);
        assertSQLException(resultSet::isLast);
        assertSQLException(() -> resultSet.relative(1));
    }

    @Test
    public void testAllNotSupported() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertSQLFeatureNotSupported(() -> resultSet.setFetchDirection(ResultSet.FETCH_REVERSE));
        assertSQLFeatureNotSupported(() -> resultSet.setFetchSize(3));
    }
}
