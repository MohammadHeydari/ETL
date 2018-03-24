package snapp.etl.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {
    private static Configuration configuration;


    private Properties properties = new Properties();

    private Configuration() {
    }

    public static Configuration getInstance() {
        if (configuration == null) {
            Configuration conf = new Configuration();
            conf.loadConfiguration();
            configuration = conf;
            return configuration;
        } else {
            return configuration;
        }
    }

    public String getConfig(String propertyName) {
        return (String) properties.get(propertyName);
    }

    protected void loadConfiguration() {
        try {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream("config.properties");
            properties.load(resourceAsStream);
        } catch (IOException e) {
            System.out.println("Config file not found.");
            System.exit(0);
        }
    }


}
