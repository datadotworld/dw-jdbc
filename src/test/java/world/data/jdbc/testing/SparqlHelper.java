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
package world.data.jdbc.testing;

import world.data.jdbc.connections.DataWorldConnection;
import world.data.jdbc.statements.DataWorldPreparedStatement;
import world.data.jdbc.statements.DataWorldStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SparqlHelper extends CloserResource {

    public DataWorldConnection connect() throws SQLException {
        String url = "jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset";
        return register((DataWorldConnection) DriverManager.getConnection(url, TestConfigSource.testProperties()));
    }

    public DataWorldStatement createStatement(Connection connection) throws SQLException {
        return register((DataWorldStatement) connection.createStatement());
    }

    public DataWorldPreparedStatement prepareStatement(Connection connection, String query) throws SQLException {
        return register((DataWorldPreparedStatement) connection.prepareStatement(query));
    }

    public ResultSet executeQuery(Statement statement, String query) throws SQLException {
        return register(statement.executeQuery(query));
    }

    public ResultSet executeQuery(PreparedStatement statement) throws SQLException {
        return register(statement.executeQuery());
    }
}
