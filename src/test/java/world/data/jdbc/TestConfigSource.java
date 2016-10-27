package world.data.jdbc;

import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class TestConfigSource {
    public static Properties testProperties() {
        final Properties out = new Properties();
        out.setProperty("user", "");
        out.setProperty("password", "token");
        out.setProperty("querybaseurl", "http://localhost:3333");
        return out;
    }
}
