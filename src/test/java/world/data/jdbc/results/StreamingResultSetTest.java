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
import java.sql.SQLException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;
import static world.data.jdbc.testing.MoreAssertions.assertSQLFeatureNotSupported;

public class StreamingResultSetTest {
    private static NanoHTTPDHandler lastBackendRequest;
    private static final String resultResourceName = "/select.json";
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
        return sparql.executeQuery(statement, "select ?s where {?s ?p ?o.}");
    }

    @Test
    public void absoluteNoMove() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.isBeforeFirst()).isTrue();
        assertThat(resultSet.isFirst()).isFalse();
        assertSQLException(() -> resultSet.absolute(0));
    }

    @Test
    public void absoluteFirst() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.absolute(1)).isTrue();
        assertThat(resultSet.absolute(1)).isTrue();
        assertThat(resultSet.getRow()).isEqualTo(1);
    }

    @Test
    public void absoluteMoveForward() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.absolute(2)).isTrue();
        assertThat(resultSet.absolute(2)).isTrue();
        assertThat(resultSet.getRow()).isEqualTo(2);
    }

    @Test
    public void absoluteMoveBack() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.absolute(3)).isTrue();
        assertThat(resultSet.getRow()).isEqualTo(3);
        assertSQLException(() -> resultSet.absolute(2));
    }

    @Test
    public void absoluteMoveLast() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.absolute(-1)).isTrue();
        assertThat(resultSet.isLast()).isTrue();
        resultSet.next();
        assertThat(resultSet.isAfterLast()).isTrue();
    }

    @Test
    public void absoluteMoveFurther() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.absolute(1000)).isFalse();
        assertThat(resultSet.isAfterLast()).isTrue();
    }

    @Test
    public void absoluteSecondLast() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertSQLException(() -> resultSet.absolute(-2));
    }

    @Test
    public void absoluteAfterLast() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.afterLast();
        assertThat(resultSet.isAfterLast()).isTrue();
        resultSet.afterLast();
        assertThat(resultSet.isAfterLast()).isTrue();
    }

    @Test
    public void absoluteBeforeFirstMoved() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertSQLException(resultSet::beforeFirst);
    }

    @Test
    public void absoluteBeforeFirstOk() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.beforeFirst();
    }

    @Test
    public void firstMoved() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.absolute(3);
        assertSQLException(resultSet::first);
    }

    @Test
    public void firstOk() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        resultSet.first();
    }

    @Test
    public void getFetchDirectionSize() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
        assertThat(resultSet.getFetchSize()).isEqualTo(0);
        assertThat(resultSet.getType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
    }

    @Test
    public void setFetchDirectionOk() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
    }

    @Test
    public void relativeZero() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.relative(0);
    }

    @Test
    public void relativeNegative() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertSQLException(() -> resultSet.relative(-2));
    }

    @Test
    public void relativeShort() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.relative(3)).isTrue();
        assertThat(resultSet.getRow()).isEqualTo(3);
    }

    @Test
    public void relativeLong() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertThat(resultSet.relative(10000)).isFalse();
    }

    @Test
    public void testAllClosed() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.close();
        assertSQLException(() -> resultSet.absolute(9));
        assertSQLException(resultSet::afterLast);
        assertSQLException(resultSet::beforeFirst);
        assertSQLException(resultSet::first);
        assertSQLException(resultSet::isAfterLast);
        assertSQLException(resultSet::isBeforeFirst);
        assertSQLException(resultSet::isFirst);
        assertSQLException(resultSet::isLast);
        assertSQLException(resultSet::last);
        assertSQLException(resultSet::next);
        assertSQLException(() -> resultSet.relative(0));
    }

    @Test
    public void testAllNotSupported() throws Exception {
        ResultSet resultSet = sampleResultSet();
        assertSQLFeatureNotSupported(() -> resultSet.setFetchDirection(ResultSet.FETCH_REVERSE));
        assertSQLFeatureNotSupported(() -> resultSet.setFetchSize(0));
    }
}
