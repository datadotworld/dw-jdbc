package world.data.jdbc;

import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class TestConfigSource {
    private static String getToken(){
        try {
            return IOUtils.toString(new FileInputStream("/Users/daveg/test.token"));
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    public static Properties testProperties() {
        final Properties out = new Properties();
        out.setProperty("user", "");
        out.setProperty("password", getToken());
        out.setProperty("querybaseurl", "http://localhost:3333");
        return out;
    }
}
