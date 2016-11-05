package ru.spbstu.frauddetection.core.storm_manager;


import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class SystemProperties {

    private static final Logger logger = Logger.getLogger(StormStarter.class);
    private static Path systemConfigPath;


    public SystemProperties() throws IOException {
        systemConfigPath = Paths.get(System.getProperty("user.dir"));
        systemConfigPath = systemConfigPath.getParent();

    }


    public Boolean getSysProperty(String key) {

        FileInputStream SysProperties;
        Properties property = new Properties();

        Boolean value=null;
        try

        {
            SysProperties = new FileInputStream(systemConfigPath.toString() + "/system.properties");
            property.load(SysProperties);
            value = Boolean.valueOf(property.getProperty(key));
            logger.info(key + " " + value);

        } catch (IOException e) {
            logger.fatal("Properties file does not exist");
        }
        return value;
    }

}







