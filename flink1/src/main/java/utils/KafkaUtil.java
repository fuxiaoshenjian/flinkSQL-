package utils;

import java.util.Properties;

public class KafkaUtil {

    public static Properties getProperties(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.0.17.66:6667");
        properties.setProperty("zookeeper.connect", "10.0.17.66:2181");
        properties.setProperty("group.id", "test");
        return properties;
    }
}
