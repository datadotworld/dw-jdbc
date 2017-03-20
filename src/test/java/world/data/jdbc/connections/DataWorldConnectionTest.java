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

import org.junit.Test;
import world.data.jdbc.TestConfigSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class DataWorldConnectionTest {

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testCreateArray() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.createArrayOf("String", new Object[0]);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testCreateBlob() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.createBlob();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testCreateClob() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.createClob();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testCreateNClob() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.createNClob();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testCreateSQLXML() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.createSQLXML();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testCreateStruct() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.createStruct("String", new Object[0]);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testGetTypeMap() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.getTypeMap();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testNativeSql() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.nativeSQL("");
    }

    @Test(expected = SQLException.class)
    public void testPrepareCallClosede() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.prepareCall("");
    }

    @Test(expected = SQLException.class)
    public void testPrepareCall2Closed() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.prepareCall("", 0, 0);
    }

    @Test(expected = SQLException.class)
    public void testPrepareCall3Closed() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.prepareCall("", 0, 0, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testReleaseSavepoint() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.releaseSavepoint(null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testRollbackSavepoint() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.rollback(null);
    }

    @Test(expected = SQLException.class)
    public void testRollbackClosed() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.rollback();
    }

    @Test
    public void testRollbackNotClosed() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.rollback();
    }

    @Test(expected = SQLException.class)
    public void testCommitClosed() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.commit();
    }

    @Test
    public void testCommitNotClosed() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.commit();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetSavepoint() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setSavepoint();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetNamedSavepoint() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setSavepoint("");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetCatalog() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setCatalog(null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetTransactionIsolation() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }

    @Test
    public void testSetTransactionIsolationOkay() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setTransactionIsolation(Connection.TRANSACTION_NONE);
    }

    @Test
    public void testSetAutocommitOkay() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setAutoCommit(true);
        connection.setAutoCommit(false);
        connection.setAutoCommit(true);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetSchema() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setSchema("foo");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testIsWrapperFor() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.isWrapperFor(this.getClass());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testUnwrap() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.unwrap(this.getClass());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetTypeMap() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setTypeMap(null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetNetworkTimeout() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setNetworkTimeout(null, 1000);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testGetNetworkTimeout() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.getNetworkTimeout();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testGetSchema() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.getSchema();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testAbort() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.abort(null);
    }

    @Test
    public void testHoldability() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Test(expected = SQLException.class)
    public void testBadHoldability() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test(expected = SQLException.class)
    public void testSetReadOnly() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setReadOnly(false);
    }

    @Test
    public void testSetReadOnlyOkay() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setReadOnly(true);
    }

    @Test(expected = SQLException.class)
    public void testSetReadOnlyClosed() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.setReadOnly(false);
    }

    @Test()
    public void testClientInfo() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.setClientInfo("foo", "bar");
        assertThat(connection.getClientInfo("foo")).isEqualTo("bar");
    }

    @Test()
    public void testClientInfoBatch() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        final Properties props = new Properties();
        props.setProperty("foo", "bar");
        connection.setClientInfo(props);
        assertThat(connection.getClientInfo("foo")).isEqualTo("bar");
        assertThat(connection.getClientInfo().getProperty("foo")).isEqualTo("bar");
    }

    @Test(expected = SQLException.class)
    public void testCreateStatementClosed() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.createStatement();
    }

    @Test(expected = SQLException.class)
    public void testCreateStatementClosed2() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.createStatement(0, 0);
    }

    @Test(expected = SQLException.class)
    public void testCreateStatementClosed3() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.createStatement(0, 0, 0);
    }

    @Test(expected = SQLException.class)
    public void testprepareStatementClosed() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.prepareStatement("");
    }

    @Test(expected = SQLException.class)
    public void testprepareStatementClosed2() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.prepareStatement("", 0, 0);
    }

    @Test(expected = SQLException.class)
    public void testprepareStatementClosed3() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.prepareStatement("", 0, 0, 0);
    }

    @Test(expected = SQLException.class)
    public void testprepareStatementAutoClosed() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.prepareStatement("", 3);
    }

    @Test(expected = SQLException.class)
    public void testprepareStatementIndexClosed() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.prepareStatement("", (int[]) null);
    }

    @Test(expected = SQLException.class)
    public void testprepareStatementColumnNamesClosed() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.close();
        connection.prepareStatement("", (String[]) null);
    }

    @Test
    public void testprepareStatementAuto() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.prepareStatement("", 3);
    }

    @Test
    public void testprepareStatementIndex() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.prepareStatement("", (int[]) null);
    }

    @Test
    public void testprepareStatementColumnNames() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.prepareStatement("", (String[]) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testprepareStatementScrolling() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.prepareStatement("", ResultSet.TYPE_SCROLL_INSENSITIVE, 0, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testprepareStatementConcurrency() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.prepareStatement("", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testCreateStatementScrolling() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 0, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testCreateStatementConcurrency() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, 0);
    }

    @Test
    public void testIsValid() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        assertThat(connection.isValid(0)).isTrue();
        connection.close();
        assertThat(connection.isValid(0)).isFalse();
    }

    @Test
    public void testIsReadOnly() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        assertThat(connection.isReadOnly()).isTrue();
    }

    @Test
    public void testGetAutocommit() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        assertThat(connection.getAutoCommit()).isTrue();
    }

    @Test
    public void testGetTransactionIsolation() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset", TestConfigSource.testProperties());
        assertThat(connection.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_NONE);
    }
}