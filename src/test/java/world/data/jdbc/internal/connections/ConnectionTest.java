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
package world.data.jdbc.internal.connections;

import org.junit.Rule;
import org.junit.Test;
import world.data.jdbc.DataWorldConnection;
import world.data.jdbc.DataWorldStatement;
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
    public void testCatalog() throws Exception {
        DataWorldConnection connection = sparql.connect();
        assertThat(connection.getCatalog()).isEqualTo("dave");

        connection.setCatalog("dave"); // no-op
        assertSQLException(() -> connection.setCatalog("foo"));
    }

    @Test
    public void testSchema() throws Exception {
        DataWorldConnection connection = sparql.connect();
        assertThat(connection.getSchema()).isEqualTo("lahman-sabremetrics-dataset");

        connection.setSchema("lahman-sabremetrics-dataset"); // no-op
        assertSQLException(() -> connection.setSchema("foo"));
    }

    @Test
    public void testRollbackNotClosed() throws SQLException {
        DataWorldConnection connection = sparql.connect();
        connection.rollback();
    }

    @Test
    public void testCommitNotClosed() throws SQLException {
        DataWorldConnection connection = sparql.connect();
        connection.commit();
    }

    @Test
    public void testSetTransactionIsolationOkay() throws SQLException {
        DataWorldConnection connection = sparql.connect();
        connection.setTransactionIsolation(DataWorldConnection.TRANSACTION_NONE);
    }

    @Test
    public void testSetAutocommitOkay() throws SQLException {
        DataWorldConnection connection = sparql.connect();
        connection.setAutoCommit(true);
        connection.setAutoCommit(false);
        connection.setAutoCommit(true);
    }

    @Test
    public void testHoldability() throws SQLException {
        DataWorldConnection connection = sparql.connect();
        connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Test
    public void testBadHoldability() throws Exception {
        DataWorldConnection connection = sparql.connect();
        assertSQLException(() -> connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    }

    @Test
    public void testSetReadOnly() throws Exception {
        DataWorldConnection connection = sparql.connect();
        assertSQLException(() -> connection.setReadOnly(false));
    }

    @Test
    public void testSetReadOnlyOkay() throws SQLException {
        DataWorldConnection connection = sparql.connect();
        connection.setReadOnly(true);
    }

    @Test()
    public void testClientInfo() throws SQLException {
        DataWorldConnection connection = sparql.connect();
        connection.setClientInfo("foo", "bar");
        assertThat(connection.getClientInfo("foo")).isEqualTo("bar");
    }

    @Test()
    public void testClientInfoBatch() throws SQLException {
        DataWorldConnection connection = sparql.connect();
        Properties props = new Properties();
        props.setProperty("foo", "bar");
        connection.setClientInfo(props);
        assertThat(connection.getClientInfo("foo")).isEqualTo("bar");
        assertThat(connection.getClientInfo().getProperty("foo")).isEqualTo("bar");
    }

    @Test
    public void testIsValid() throws SQLException {
        DataWorldConnection connection = sparql.connect();
        assertThat(connection.isValid(0)).isTrue();
        connection.close();
        assertThat(connection.isValid(0)).isFalse();
    }

    @Test
    public void testIsReadOnly() throws SQLException {
        DataWorldConnection connection = sparql.connect();
        assertThat(connection.isReadOnly()).isTrue();
    }

    @Test
    public void testGetAutocommit() throws SQLException {
        DataWorldConnection connection = sparql.connect();
        assertThat(connection.getAutoCommit()).isTrue();
    }

    @Test
    public void testGetTransactionIsolation() throws SQLException {
        DataWorldConnection connection = sparql.connect();
        assertThat(connection.getTransactionIsolation()).isEqualTo(DataWorldConnection.TRANSACTION_NONE);
    }

    @Test
    public void testWrapperFor() throws SQLException {
        DataWorldConnection connection = sparql.connect();
        assertThat(connection.isWrapperFor(DataWorldConnection.class)).isTrue();
        assertThat(connection.isWrapperFor(DataWorldStatement.class)).isFalse();
        assertThat(connection.unwrap(DataWorldConnection.class)).isSameAs(connection);
    }

    @Test
    public void testAllClosed() throws Exception {
        DataWorldConnection connection = sparql.connect();
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
        DataWorldConnection connection = sparql.connect();
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
        assertSQLFeatureNotSupported(connection::getTypeMap);
        assertSQLFeatureNotSupported(() -> connection.nativeSQL(""));
        assertSQLFeatureNotSupported(() -> connection.prepareStatement("", (String[]) null));
        assertSQLFeatureNotSupported(() -> connection.prepareStatement("", (int[]) null));
        assertSQLFeatureNotSupported(() -> connection.prepareStatement("", 3));
        assertSQLFeatureNotSupported(() -> connection.prepareStatement("", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, 0));
        assertSQLFeatureNotSupported(() -> connection.prepareStatement("", ResultSet.TYPE_SCROLL_INSENSITIVE, 0, 0));
        assertSQLFeatureNotSupported(() -> connection.releaseSavepoint(null));
        assertSQLFeatureNotSupported(() -> connection.rollback(null));
        assertSQLFeatureNotSupported(() -> connection.setNetworkTimeout(null, 1000));
        assertSQLFeatureNotSupported(() -> connection.setSavepoint(""));
        assertSQLFeatureNotSupported(connection::setSavepoint);
        assertSQLFeatureNotSupported(() -> connection.setTransactionIsolation(DataWorldConnection.TRANSACTION_READ_COMMITTED));
        assertSQLFeatureNotSupported(() -> connection.setTypeMap(null));
    }
}
