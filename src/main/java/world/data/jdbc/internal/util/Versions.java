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
package world.data.jdbc.internal.util;


import lombok.extern.java.Log;
import world.data.jdbc.DataWorldDriver;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;

@Log
public final class Versions {

    public static String findVersionString() {
        // Parse the properties file that Maven builds create automatically
        String resourceName = "META-INF/maven/world.data/dw-jdbc/pom.properties";
        URL resourceUrl = DataWorldDriver.class.getClassLoader().getResource(resourceName);
        if (resourceUrl == null) {
            // Usually this is because we're running in a dev environment that doesn't build via Maven
            log.info("Unable to find JDBC driver version via resource: " + resourceName);
            return "0.0-UNKNOWN";
        }
        Properties props = new Properties();
        try (InputStream in = new BufferedInputStream(resourceUrl.openStream())) {
            props.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to parse resource: " + resourceName, e);
        }
        String version = props.getProperty("version");
        if (version == null) {
            throw new IllegalStateException("Unable find 'version' property in resource: " + resourceName);
        }
        return version;
    }

    public static int[] parseVersionNumbers(String version) {
        // Ignore any text suffix (eg. "-SNAPSHOT")
        String versionNumbers = version.replaceFirst("^([0-9.]+).*", "$1");
        int[] versions = Arrays.stream(versionNumbers.split("\\.")).mapToInt(Integer::parseInt).toArray();
        if (versions.length < 2) {
            throw new IllegalStateException("Expected version number to contain at least <major>.<minor>.");
        }
        return versions;
    }
}
