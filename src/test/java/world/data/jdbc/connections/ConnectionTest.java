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
package world.data.jdbc.connections;

import org.junit.Rule;
import org.junit.Test;
import world.data.jdbc.testing.SparqlHelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;
import static world.data.jdbc.testing.MoreAssertions.assertSQLFeatureNotSupported;

public class ConnectionTest {

    @Rule
    public final SparqlHelper sparql = new SparqlHelper();

    @Test
    public void testRollbackNotClosed() throws SQLException {
        Connection connection = sparql.connect();
        connection.rollback();
    }

    @Test
    public void testCommitNotClosed() throws SQLException {
        Connection connection = sparql.connect();
        connection.commit();
    }

    @Test
    public void testSetTransactionIsolationOkay() throws SQLException {
        Connection connection = sparql.connect();
        connection.setTransactionIsolation(Connection.TRANSACTION_NONE);
    }

    @Test
    public void testSetAutocommitOkay() throws SQLException {
        Connection connection = sparql.connect();
        connection.setAutoCommit(true);
        connection.setAutoCommit(false);
        connection.setAutoCommit(true);
    }

    @Test
    public void testHoldability() throws SQLException {
        Connection connection = sparql.connect();
        connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Test
    public void testBadHoldability() throws Exception {
        Connection connection = sparql.connect();
        assertSQLException(() -> connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    }

    @Test
    public void testSetReadOnly() throws Exception {
        Connection connection = sparql.connect();
        assertSQLException(() -> connection.setReadOnly(false));
    }

    @Test
    public void testSetReadOnlyOkay() throws SQLException {
        Connection connection = sparql.connect();
        connection.setReadOnly(true);
    }

    @Test()
    public void testClientInfo() throws SQLException {
        Connection connection = sparql.connect();
        connection.setClientInfo("foo", "bar");
        assertThat(connection.getClientInfo("foo")).isEqualTo("bar");
    }

    @Test()
    public void testClientInfoBatch() throws SQLException {
        Connection connection = sparql.connect();
        Properties props = new Properties();
        props.setProperty("foo", "bar");
        connection.setClientInfo(props);
        assertThat(connection.getClientInfo("foo")).isEqualTo("bar");
        assertThat(connection.getClientInfo().getProperty("foo")).isEqualTo("bar");
    }

    @Test
    public void testPrepareStatementAuto() throws SQLException {
        Connection connection = sparql.connect();
        connection.prepareStatement("", 3);
    }

    @Test
    public void testPrepareStatementIndex() throws SQLException {
        Connection connection = sparql.connect();
        connection.prepareStatement("", (int[]) null);
    }

    @Test
    public void testPrepareStatementColumnNames() throws SQLException {
        Connection connection = sparql.connect();
        connection.prepareStatement("", (String[]) null);
    }

    @Test
    public void testIsValid() throws SQLException {
        Connection connection = sparql.connect();
        assertThat(connection.isValid(0)).isTrue();
        connection.close();
        assertThat(connection.isValid(0)).isFalse();
    }

    @Test
    public void testIsReadOnly() throws SQLException {
        Connection connection = sparql.connect();
        assertThat(connection.isReadOnly()).isTrue();
    }

    @Test
    public void testGetAutocommit() throws SQLException {
        Connection connection = sparql.connect();
        assertThat(connection.getAutoCommit()).isTrue();
    }

    @Test
    public void testGetTransactionIsolation() throws SQLException {
        Connection connection = sparql.connect();
        assertThat(connection.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_NONE);
    }

    @Test
    public void testAllClosed() throws Exception {
        Connection connection = sparql.connect();
        connection.close();
        assertSQLException(connection::commit);
        assertSQLException(connection::createStatement);
        assertSQLException(() -> connection.createStatement(0, 0));
        assertSQLException(() -> connection.createStatement(0, 0, 0));
        assertSQLException(() -> connection.prepareCall(""));
        assertSQLException(() -> connection.prepareCall("", 0, 0));
        assertSQLException(() -> connection.prepareCall("", 0, 0, 0));
        assertSQLException(() -> connection.prepareStatement(""));
        assertSQLException(() -> connection.prepareStatement("", (String[]) null));
        assertSQLException(() -> connection.prepareStatement("", (int[]) null));
        assertSQLException(() -> connection.prepareStatement("", 0, 0));
        assertSQLException(() -> connection.prepareStatement("", 0, 0, 0));
        assertSQLException(() -> connection.prepareStatement("", 3));
        assertSQLException(connection::rollback);
        assertSQLException(() -> connection.setReadOnly(false));
    }

    @Test
    public void testAllNotSupported() throws Exception {
        Connection connection = sparql.connect();
        assertSQLFeatureNotSupported(() -> connection.abort(null));
        assertSQLFeatureNotSupported(() -> connection.createArrayOf("String", new Object[0]));
        assertSQLFeatureNotSupported(connection::createBlob);
        assertSQLFeatureNotSupported(connection::createClob);
        assertSQLFeatureNotSupported(connection::createNClob);
        assertSQLFeatureNotSupported(connection::createSQLXML);
        assertSQLFeatureNotSupported(() -> connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, 0));
        assertSQLFeatureNotSupported(() -> connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 0, 0));
        assertSQLFeatureNotSupported(() -> connection.createStruct("String", new Object[0]));
        assertSQLFeatureNotSupported(connection::getNetworkTimeout);
        assertSQLFeatureNotSupported(connection::getSchema);
        assertSQLFeatureNotSupported(connection::getTypeMap);
        assertSQLFeatureNotSupported(() -> connection.isWrapperFor(getClass()));
        assertSQLFeatureNotSupported(() -> connection.nativeSQL(""));
        assertSQLFeatureNotSupported(() -> connection.prepareStatement("", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, 0));
        assertSQLFeatureNotSupported(() -> connection.prepareStatement("", ResultSet.TYPE_SCROLL_INSENSITIVE, 0, 0));
        assertSQLFeatureNotSupported(() -> connection.releaseSavepoint(null));
        assertSQLFeatureNotSupported(() -> connection.rollback(null));
        assertSQLFeatureNotSupported(() -> connection.setCatalog(null));
        assertSQLFeatureNotSupported(() -> connection.setNetworkTimeout(null, 1000));
        assertSQLFeatureNotSupported(() -> connection.setSavepoint(""));
        assertSQLFeatureNotSupported(connection::setSavepoint);
        assertSQLFeatureNotSupported(() -> connection.setSchema("foo"));
        assertSQLFeatureNotSupported(() -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
        assertSQLFeatureNotSupported(() -> connection.setTypeMap(null));
        assertSQLFeatureNotSupported(() -> connection.unwrap(getClass()));
    }
}
