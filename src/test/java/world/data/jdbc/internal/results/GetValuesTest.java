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
package world.data.jdbc.internal.results;

import fi.iki.elonen.NanoHTTPD;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import world.data.jdbc.DataWorldStatement;
import world.data.jdbc.JdbcCompatibility;
import world.data.jdbc.testing.NanoHTTPDHandler;
import world.data.jdbc.testing.NanoHTTPDResource;
import world.data.jdbc.testing.SqlHelper;
import world.data.jdbc.testing.Utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class GetValuesTest {
    private static NanoHTTPDHandler lastBackendRequest;
    private static final String resultResourceName = "/hall_of_fame.json";
    private static final String resultMimeType = Utils.TYPE_SPARQL_RESULTS;

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
    public final SqlHelper sql = new SqlHelper();

    @Before
    public void setup() {
        lastBackendRequest = mock(NanoHTTPDHandler.class);
    }

    private ResultSet sampleResultSet() throws SQLException {
        DataWorldStatement statement = sql.createStatement(sql.connect());
        return sql.executeQuery(statement, "select * from HallOfFame limit 10");
    }

    @Test
    public void getBigDecimal() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getBigDecimal(2)).isEqualTo(new BigDecimal(1936));
        assertThat(resultSet.getBigDecimal("yearid")).isEqualTo(new BigDecimal(1936));
        assertThat(resultSet.wasNull()).isFalse();
        assertThat(resultSet.getBigDecimal("null_col")).isNull();
        assertThat(resultSet.wasNull()).isTrue();
    }

    @Test
    public void getBoolean1() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getBoolean(7)).isEqualTo(true);
        assertThat(resultSet.getBoolean("inducted")).isEqualTo(true);
        assertThat(resultSet.wasNull()).isFalse();
        assertThat(resultSet.getBoolean("null_col")).isEqualTo(false);
        assertThat(resultSet.wasNull()).isTrue();
    }

    @Test
    public void getByte() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getByte(6)).isEqualTo((byte) 55);
        assertThat(resultSet.getByte("votes")).isEqualTo((byte) 55);
        assertThat(resultSet.wasNull()).isFalse();
        assertThat(resultSet.getByte("null_col")).isEqualTo((byte) 0);
        assertThat(resultSet.wasNull()).isTrue();
    }

    @Test
    public void getDate() throws Exception {

    }

    @Test
    public void getDate1() throws Exception {

    }

    @Test
    public void getDouble() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getDouble(2)).isEqualTo(1936.0);
        assertThat(resultSet.getDouble("yearid")).isEqualTo(1936.0);
        assertThat(resultSet.wasNull()).isFalse();
        assertThat(resultSet.getDouble("null_col")).isEqualTo(0.0);
        assertThat(resultSet.wasNull()).isTrue();
    }

    @Test
    public void getFloat() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getFloat(2)).isEqualTo(1936.0F);
        assertThat(resultSet.getFloat("yearid")).isEqualTo(1936.0F);
        assertThat(resultSet.wasNull()).isFalse();
        assertThat(resultSet.getFloat("null_col")).isEqualTo(0.0F);
        assertThat(resultSet.wasNull()).isTrue();
    }

    @Test
    public void getInt() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getInt(2)).isEqualTo(1936);
        assertThat(resultSet.getInt("yearid")).isEqualTo(1936);
        assertThat(resultSet.wasNull()).isFalse();
        assertThat(resultSet.getInt("null_col")).isEqualTo(0);
        assertThat(resultSet.wasNull()).isTrue();
    }

    @Test
    public void getLong() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getLong(2)).isEqualTo(1936L);
        assertThat(resultSet.getLong("yearid")).isEqualTo(1936L);
        assertThat(resultSet.wasNull()).isFalse();
        assertThat(resultSet.getLong("null_col")).isEqualTo(0L);
        assertThat(resultSet.wasNull()).isTrue();
    }

    @Test
    public void getNString() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getNString(3)).isEqualTo("BBWAA");
        assertThat(resultSet.getNString("votedBy")).isEqualTo("BBWAA");
        assertThat(resultSet.wasNull()).isFalse();
        assertThat(resultSet.getNString("null_col")).isEqualTo(null);
        assertThat(resultSet.wasNull()).isTrue();
    }

    @Test
    public void getObject() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getObject(1)).isEqualTo("alexape01");
        assertThat(resultSet.getObject(2)).isEqualTo(BigInteger.valueOf(1936));
        assertThat(resultSet.getObject(2, Integer.class)).isEqualTo(1936);
        assertThat(resultSet.getObject("yearid")).isEqualTo(BigInteger.valueOf(1936));
        assertThat(resultSet.wasNull()).isFalse();
        assertThat(resultSet.getObject("null_col")).isEqualTo(null);
        assertThat(resultSet.wasNull()).isTrue();
    }

    @Test
    public void getObjectHiCompatability() throws Exception {
        DataWorldStatement statement = sql.createStatement(sql.connect());
        statement.setJdbcCompatibilityLevel(JdbcCompatibility.HIGH);
        ResultSet resultSet = sql.executeQuery(statement, "select * from HallOfFame limit 10");
        resultSet.next();
        assertThat(resultSet.getObject(1)).isEqualTo("alexape01");
        assertThat(resultSet.getObject(2)).isEqualTo(BigInteger.valueOf(1936));
        assertThat(resultSet.getObject("yearid")).isEqualTo(BigInteger.valueOf(1936));
        assertThat(resultSet.wasNull()).isFalse();
        assertThat(resultSet.getObject("null_col")).isEqualTo(null);
        assertThat(resultSet.wasNull()).isTrue();
    }

    @Test
    public void getShort() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getShort(2)).isEqualTo((short) 1936);
        assertThat(resultSet.getShort("yearid")).isEqualTo((short) 1936);
        assertThat(resultSet.wasNull()).isFalse();
        assertThat(resultSet.getShort("null_col")).isEqualTo((short) 0);
        assertThat(resultSet.wasNull()).isTrue();
    }

    @Test
    public void getString() throws Exception {
        ResultSet resultSet = sampleResultSet();
        resultSet.next();
        assertThat(resultSet.getString(3)).isEqualTo("BBWAA");
        assertThat(resultSet.getString("votedBy")).isEqualTo("BBWAA");
        assertThat(resultSet.wasNull()).isFalse();
        assertThat(resultSet.getString("null_col")).isEqualTo(null);
        assertThat(resultSet.wasNull()).isTrue();
    }

    @Test
    public void getTime() throws Exception {

    }

    @Test
    public void getTime1() throws Exception {

    }

    @Test
    public void getTimestamp() throws Exception {

    }

    @Test
    public void getTimestamp1() throws Exception {

    }
}
