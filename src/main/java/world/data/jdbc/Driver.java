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
package world.data.jdbc;

import lombok.extern.java.Log;
import world.data.jdbc.internal.connections.ConnectionImpl;
import world.data.jdbc.internal.query.QueryEngine;
import world.data.jdbc.internal.query.SparqlEngine;
import world.data.jdbc.internal.query.SqlEngine;
import world.data.jdbc.internal.transport.HttpQueryApi;
import world.data.jdbc.internal.transport.QueryApi;
import world.data.jdbc.internal.util.Versions;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static world.data.jdbc.internal.util.Conditions.check;

/**
 * A JDBC driver for SQL and SPARQL queries against datasets hosted on <a href="https://data.world">data.world</a>.
 */
@Log
@SuppressWarnings("WeakerAccess")
public final class Driver implements java.sql.Driver {

    /**
     * Constant for the primary JDBC Driver prefix, implementations supply
     * an additional prefix which will form the next portion of the JDBC URL
     */
    public static final String SQL_PREFIX = "jdbc:data:world:sql:";

    public static final String SPARQL_PREFIX = "jdbc:data:world:sparql:";

    /**
     * Constant for the standard JDBC connection URL parameter used to set
     * password for drivers that support authentication
     */
    public static final String PARAM_PASSWORD = "password";

    /**
     * Constant for the connection URL parameter which sets the desired {@link JdbcCompatibility} level.
     */
    public static final String PARAM_JDBC_COMPATIBILITY = "jdbcCompatibility";

    public static final String VERSION = Versions.findVersionString();

    private static final int[] VERSION_NUMBERS = Versions.parseVersionNumbers(VERSION);

    static {
        try {
            register();
        } catch (SQLException e) {
            log.log(Level.WARNING, "Unable to register data.world JDBC driver", e);
        }
    }

    public static synchronized void register() throws SQLException {
        DriverManager.registerDriver(new Driver());
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(SQL_PREFIX) || url.startsWith(SPARQL_PREFIX);
    }

    @Override
    public DataWorldConnection connect(String url, Properties props) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        Properties effectiveProps = new Properties();
        String[] urlParts = url.split(";");
        String[] urlFields = urlParts[0].split(":", 6);
        check(urlFields.length == 6, "Invalid jdbc url, expected 'jdbc:data:world:<language>:<account>:<dataset>': %s", url);
        effectiveProps.setProperty("lang", urlFields[3]);
        effectiveProps.setProperty("agentid", urlFields[4]);
        effectiveProps.setProperty("datasetid", urlFields[5]);
        effectiveProps.setProperty("querybaseurl", "https://query.data.world");
        if (props != null) {
            for (String key : props.stringPropertyNames()) {
                String value = props.getProperty(key);
                effectiveProps.setProperty(key.toLowerCase(), value);
            }
        }
        for (int i = 1; i < urlParts.length; i++) {
            String[] pair = urlParts[i].split("=", 2);
            check(pair.length == 2, "Invalid jdbc url, expected ';name=value' pairs in suffix: %s", url);
            effectiveProps.setProperty(pair[0].toLowerCase(), urlDecode(pair[1]));
        }

        String queryBaseUrl = effectiveProps.getProperty("querybaseurl");
        String lang = effectiveProps.getProperty("lang");
        String agentId = effectiveProps.getProperty("agentid");
        String datasetId = effectiveProps.getProperty("datasetid");
        String password = effectiveProps.getProperty(PARAM_PASSWORD);
        JdbcCompatibility jdbcCompatibility = getProperty(effectiveProps, PARAM_JDBC_COMPATIBILITY, JdbcCompatibility.class);

        // Create the QueryApi responsible for low-level HTTP details
        URL queryEndpoint = getQueryEndpoint(queryBaseUrl, lang, agentId, datasetId);
        String userAgent = String.format("DwJdbc-%s/%s", lang, Driver.VERSION);
        QueryApi queryApi = new HttpQueryApi(queryEndpoint, userAgent, password);

        // Create the QueryEngine responsible for query language-specific behavior
        QueryEngine queryEngine;
        if ("sparql".equals(lang)) {
            queryEngine = new SparqlEngine(queryApi, agentId, datasetId);
        } else if ("sql".equals(lang)) {
            queryEngine = new SqlEngine(queryApi, agentId, datasetId);
        } else {
            throw new SQLException("Unknown query language: " + lang);
        }

        return new ConnectionImpl(queryEngine, jdbcCompatibility);
    }

    private static URL getQueryEndpoint(String queryBaseUrl, String lang, String agentId, String datasetId) throws SQLException {
        String queryUrl = String.format("%s/%s/%s/%s", queryBaseUrl, lang, agentId, datasetId);
        try {
            return new URL(queryUrl);
        } catch (MalformedURLException e) {
            throw new SQLException("Bad query service url: " + queryUrl, e);
        }
    }

    private static <E extends Enum<E>> E getProperty(Properties props, String key, Class<E> enumClass) {
        String value = props.getProperty(key.toLowerCase());
        return value != null && !value.isEmpty() ? Enum.valueOf(enumClass, value.toUpperCase()) : null;
    }

    private static String urlDecode(String string) {
        try {
            return URLDecoder.decode(string, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
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
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    /**
     * Returns that a data.world JDBC driver is not JDBC compliant since strict JDBC
     * compliance requires entry-level support for SQL-92 and we
     * don't meet that criteria
     */
    @Override
    public boolean jdbcCompliant() {
        // This has to be false since we are not fully SQL-92 compliant (no ddl, no updates)
        return false;
    }

    // Java6/7 compatibility
    @Override
    public Logger getParentLogger() {
        return Logger.getLogger(getClass().getPackage().getName());
    }
}
