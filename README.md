# dw-jdbc

dw-jdbc is a JDBC driver for connecting to datasets hosted on data.world.
It can be used to provide read-only access to any dataset provided by data.world
from any JVM language.  dw-jdbc supports query access both in dwSQL 
(data.world's SQL dialect) and in SPARQL 1.1, the native query language
for semantic web data sources.


## JDBC URLs

JDBC connects to data source based on a provided JDBC url.  data.world 
JDBC urls have the form

jdbc:data:world:[language]:[user id]:[dataset id]

where [language] is either "sql" or "sparql",[user id] is the data.world
id of the dataset owner, and [dataset id] is the data.world identifier for 
the dataset.

## Sample code (Java 8)

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;


final String QUERY = "select * from HallOfFame where playerID = ? order by yearid, playerID limit 10";
final String URL = "jdbc:data:world:sql:dave:lahman-sabremetrics-dataset";


try (final Connection connection =    // get a connection to the database, which will automatically be closed when done
         DriverManager.getConnection(URL, "<your user name>", "<your API token>"); 
     final PreparedStatement statement = // get a connection to the database, which will automatically be closed when done
         connection.prepareStatement(QUERY)) {
    statement.setString(1, "alexape01"); //bind a query parameter
    try (final ResultSet resultSet = statement.executeQuery()) { //execute the query
        ResultSetMetaData rsmd = resultSet.getMetaData();  //print out the column headers
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) System.out.print(",  ");
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println("");
        while (resultSet.next()) { //loop through the query results
            for (int i = 1; i <= columnsNumber; i++) { //print out the column headers
                if (i > 1) System.out.print(",  ");
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue);
            }
            System.out.println("");
        }
    }
}
```

## Using dw-jdbc in your project

If using Maven, you can use dw-jdbc by just including the following in your pom.xml file:

```xml
<dependency>
    <groupId>world.data</groupId>
    <artifactId>dw-jdbc</artifactId>
    <version>0.1.1</version>
</dependency>
```

For some database tools it's easier to install the jdbc driver if it's a single jar.  For this reason we also
provide dw-jdbc bundled with all its dependencies under the following:

```xml
<dependency>
    <groupId>world.data</groupId>
    <artifactId>dw-jdbc</artifactId>
    <classifier>shaded</classifier>
    <version>0.2</version>
</dependency>
```


## Finding your Token

1. Visit https://data.world
2. Visit your user settings, and click the advanced tab.
3. Copy your token.
