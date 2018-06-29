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
package world.data.jdbc.testing;

import org.apache.commons.dbcp2.BasicDataSource;
import world.data.jdbc.DataWorldCallableStatement;
import world.data.jdbc.DataWorldConnection;
import world.data.jdbc.DataWorldPreparedStatement;
import world.data.jdbc.DataWorldStatement;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SqlHelper extends CloserResource {

    public String urlPath() {
        return "/sql/dave/lahman-sabremetrics-dataset";
    }

    public DataWorldConnection connect() throws SQLException {
        String url = "jdbc:data:world:sql:dave:lahman-sabremetrics-dataset";
        return register((DataWorldConnection) DriverManager.getConnection(url, TestConfigSource.testProperties()));
    }

    public DataWorldStatement createStatement(DataWorldConnection connection) throws SQLException {
        return register(connection.createStatement());
    }

    public DataWorldPreparedStatement prepareStatement(DataWorldConnection connection, String query) throws SQLException {
        return register(connection.prepareStatement(query));
    }

    public DataWorldCallableStatement prepareCall(DataWorldConnection connection, String query) throws SQLException {
        return register(connection.prepareCall(query));
    }

    public ResultSet executeQuery(DataWorldStatement statement, String query) throws SQLException {
        return register(statement.executeQuery(query));
    }

    public ResultSet executeQuery(DataWorldPreparedStatement statement) throws SQLException {
        return register(statement.executeQuery());
    }

    public DataSource createPool() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("world.data.jdbc.Driver");
        ds.setUrl("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset");
        TestConfigSource.testProperties().forEach((k, v) -> ds.addConnectionProperty((String) k, (String) v));
        ds.setDefaultReadOnly(false);
        ds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return register(ds);
    }
}
