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

import org.apache.jena.atlas.web.auth.HttpAuthenticator;
import world.data.jdbc.connections.DataWorldConnection;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class DataWorldJdbcDriver implements Driver {

    static {
        try {
            register();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static synchronized void register() throws SQLException {
        DriverManager.registerDriver(new DataWorldJdbcDriver());
    }

    private int majorVer, minorVer;

    public DataWorldJdbcDriver() {
        this.majorVer = 0;
        this.minorVer = 1;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:data:world:sql:") || url.startsWith("jdbc:data:world:sparql:");
    }

    @SuppressWarnings("unchecked")
    @Override
    public final Connection connect(String url, Properties props) throws SQLException {
        if (!this.acceptsURL(url))
            return null;

        final Properties effectiveProps = new Properties();
        final String[] split = url.split(":");
        effectiveProps.setProperty("lang", split[3]);
        effectiveProps.setProperty("agentid", split[4]);
        effectiveProps.setProperty("datasetid", split[5]);
        effectiveProps.setProperty("querybaseurl", "https://query.data.world");
        if (props != null) {
            for (Map.Entry<Object, Object> e : props.entrySet()) {
                String key = e.getKey().toString().toLowerCase(Locale.ENGLISH);
                Object value = e.getValue();
                effectiveProps.put(key, value);
            }
        }

        String queryEndpoint = String.format("%s/%s/%s/%s",
                effectiveProps.get("querybaseurl"),
                effectiveProps.get("lang"),
                effectiveProps.get("agentid"),
                effectiveProps.get("datasetid"));
        effectiveProps.put("query", queryEndpoint);

        HttpAuthenticator authenticator = this.configureAuthenticator(effectiveProps);
        return new DataWorldConnection(effectiveProps.getProperty("query"), authenticator, effectiveProps.getProperty("lang"));
    }

    @Override
    public int getMajorVersion() {
        return this.minorVer;
    }

    @Override
    public int getMinorVersion() {
        return this.majorVer;
    }

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
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    private HttpAuthenticator configureAuthenticator(final Properties props) {
        final String username = (String) props.get("user");
        final String password = (String) props.get("password");
        final DataWorldHttpAuthenticator authenticator = new DataWorldHttpAuthenticator(username, password);
        props.put("authenticator", authenticator);
        return authenticator;
    }

}
