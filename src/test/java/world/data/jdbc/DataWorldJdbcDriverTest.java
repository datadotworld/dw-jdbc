package world.data.jdbc;

import org.junit.Test;

import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class DataWorldJdbcDriverTest {
    @Test
    public void acceptsURL() throws Exception {
        final DataWorldJdbcDriver driver = new DataWorldJdbcDriver();
        assertThat(driver.acceptsURL("jdbc:data:world:sparql:dave:lahman-sabremetrics-dataset")).isTrue();
        assertThat(driver.acceptsURL("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset")).isTrue();
        assertThat(driver.acceptsURL("mysql:dave:lahman-sabremetrics-dataset")).isFalse();
    }

    @Test
    public void connect() throws Exception {
        final DataWorldJdbcDriver driver = new DataWorldJdbcDriver();
        assertThat(driver.connect("mysql:dave:lahman-sabremetrics-dataset", null)).isNull();
    }

    @Test
    public void connectWithOverride() throws Exception {
        final DataWorldJdbcDriver driver = new DataWorldJdbcDriver();
        final Properties props = new Properties();
        props.setProperty("queryBaseUrl", "http://localhost:9092");
        assertThat(driver.connect("jdbc:data:world:sql:dave:lahman-sabremetrics-dataset", props)).isNotNull();
    }

    @Test
    public void getMajorVersion() throws Exception {
        final DataWorldJdbcDriver driver = new DataWorldJdbcDriver();
        assertThat(driver.getMajorVersion()).isEqualTo(1);

    }

    @Test
    public void getMinorVersion() throws Exception {
        final DataWorldJdbcDriver driver = new DataWorldJdbcDriver();
        assertThat(driver.getMinorVersion()).isEqualTo(0);
    }

    @Test
    public void getPropertyInfo() throws Exception {
        final DataWorldJdbcDriver driver = new DataWorldJdbcDriver();
        assertThat(driver.getPropertyInfo(null, null).length).isEqualTo(0);
    }

    @Test
    public void jdbcCompliant() throws Exception {
        final DataWorldJdbcDriver driver = new DataWorldJdbcDriver();
        assertThat(driver.jdbcCompliant()).isEqualTo(false);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getParentLogger() throws Exception {
        final DataWorldJdbcDriver driver = new DataWorldJdbcDriver();
        driver.getParentLogger();
    }

}