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

import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static world.data.jdbc.testing.MoreAssertions.assertSQLFeatureNotSupported;

public class DataWorldDriverTest {
    @Test
    public void acceptsURL() throws Exception {
        DataWorldDriver driver = new DataWorldDriver();
        assertThat(driver.acceptsURL("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset")).isTrue();
        assertThat(driver.acceptsURL("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset")).isTrue();
        assertThat(driver.acceptsURL("mysql:dave:lahman-sabremetrics-dataset")).isFalse();
    }

    @Test
    public void connect() throws Exception {
        DataWorldDriver driver = new DataWorldDriver();
        assertThat(driver.connect("mysql:dave:lahman-sabremetrics-dataset", null)).isNull();
    }

    @Test
    public void connectWithOverride() throws Exception {
        DataWorldDriver driver = new DataWorldDriver();
        Properties props = new Properties();
        props.setProperty("queryBaseUrl", "http://localhost:9092");
        assertThat(driver.connect("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", props)).isNotNull();
    }

    @Test
    public void getMajorVersion() throws Exception {
        DataWorldDriver driver = new DataWorldDriver();
        if (isMavenBuild()) {
            assertThat(driver.getMajorVersion()).isGreaterThan(0);
        } else {
            assertThat(driver.getMajorVersion()).isEqualTo(0);
        }
    }

    @Test
    public void getMinorVersion() throws Exception {
        DataWorldDriver driver = new DataWorldDriver();
        if (isMavenBuild()) {
            assertThat(driver.getMajorVersion()).isGreaterThanOrEqualTo(0);
        } else {
            assertThat(driver.getMinorVersion()).isEqualTo(0);
        }
    }

    @Test
    public void getPropertyInfo() throws Exception {
        DataWorldDriver driver = new DataWorldDriver();
        assertThat(driver.getPropertyInfo(null, null).length).isEqualTo(0);
    }

    @Test
    public void jdbcCompliant() throws Exception {
        DataWorldDriver driver = new DataWorldDriver();
        assertThat(driver.jdbcCompliant()).isEqualTo(false);
    }

    @Test
    public void testAllNotSupported() throws Exception {
        DataWorldDriver driver = new DataWorldDriver();
        assertSQLFeatureNotSupported(driver::getParentLogger);
    }

    private boolean isMavenBuild() {
        String resourceName = "/META-INF/maven/world.data/dw-jdbc/pom.properties";
        return getClass().getResource(resourceName) != null;
    }
}
