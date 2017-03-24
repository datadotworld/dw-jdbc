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
package world.data.jdbc;

import world.data.jdbc.connections.Connection;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Locale;
import java.util.Properties;

public class JdbcDriver implements Driver {

    /**
     * Constant for the primary Jena JDBC Driver prefix, implementations supply
     * an additional prefix which will form the next portion of the JDBC URL
     */
    public static final String DRIVER_PREFIX = "jdbc:data:world:";

    /**
     * Constant for the connection URL parameter which sets the desired JDBC
     * compatibility level
     */
    public static final String PARAM_JDBC_COMPATIBILITY = "jdbc-compatibility";

    /**
     * Constant for the standard JDBC connection URL parameter used to set
     * password for drivers that support authentication
     */
    public static final String PARAM_PASSWORD = "password";


    public static final String VERSION = Versions.findVersionString();

    private static final int[] VERSION_NUMBERS = Versions.parseVersionNumbers(VERSION);

    static {
        try {
            register();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static synchronized void register() throws SQLException {
        DriverManager.registerDriver(new JdbcDriver());
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:data:world:sparql:") ||
                url.startsWith("jdbc:data:world:sql:");
    }

    @Override
    public final Connection connect(String url, Properties props) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        final Properties effectiveProps = new Properties();
        final String[] split = url.split(":");
        effectiveProps.setProperty("lang", split[3]);
        effectiveProps.setProperty("agentid", split[4]);
        effectiveProps.setProperty("datasetid", split[5]);
        effectiveProps.setProperty("querybaseurl", "https://query.data.world");
        if (props != null) {
            for (String key : props.stringPropertyNames()) {
                String value = props.getProperty(key);
                effectiveProps.setProperty(key.toLowerCase(Locale.ENGLISH), value);
            }
        }

        String queryBaseUrl = effectiveProps.getProperty("querybaseurl");
        String lang = effectiveProps.getProperty("lang");
        String agentId = effectiveProps.getProperty("agentid");
        String datasetId = effectiveProps.getProperty("datasetid");
        String password = effectiveProps.getProperty(PARAM_PASSWORD);

        String queryEndpoint = String.format("%s/%s/%s/%s", queryBaseUrl, lang, agentId, datasetId);

        return new Connection(queryEndpoint, lang, password);
    }

    @Override
    public int getMajorVersion() {
        return VERSION_NUMBERS[0];
    }

    @Override
    public int getMinorVersion() {
        return VERSION_NUMBERS[1];
    }

    @Override
    public final DriverPropertyInfo[] getPropertyInfo(String url, Properties props) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    /**
     * Returns that a data.world JDBC driver is not JDBC compliant since strict JDBC
     * compliance requires entry-level support for SQL-92 and we
     * don't meet that criteria
     */
    @Override
    public final boolean jdbcCompliant() {
        // This has to be false since we are not fully SQL-92 compliant (no ddl, no updates)
        return false;
    }

    // Java6/7 compatibility
    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
