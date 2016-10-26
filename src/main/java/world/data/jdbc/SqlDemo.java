package world.data.jdbc;

import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SqlDemo {
    public static void main(String[] args) throws SQLException, IOException {

        final String token = IOUtils.toString(new FileInputStream("/Users/daveg/test.token"));
        try (final Connection connection = DriverManager.getConnection("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", "", token);
             final PreparedStatement statement = connection.prepareStatement("select * from HallOfFame where playerID = ? order by yearid, playerID limit 10")) {
            statement.setString(1, "alexape01");
            try (final ResultSet resultSet = statement.executeQuery()) {
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
        }
    }
}
