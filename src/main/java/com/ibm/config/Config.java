package com.ibm.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class Config
{
    final String javaConfigPropertiesPath =
            Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("sampler.properties")).getPath();
    private final Properties appProps = new Properties();

    public void loadViaJavaProperties()
            throws IOException
    {
        appProps.load(new FileInputStream(javaConfigPropertiesPath));
    }

    public void setConf(String key, Long value)
    {
        appProps.setProperty(key, value.toString());
    }

    // First priority to system properties.
    public String getConf(String key)
    {
        if (Objects.nonNull(System.getProperty(key))) {
            return System.getProperty(key);
        }
        return appProps.getProperty(key);
    }

    public String getTableName(String dbType)
    {
        return getRequiredConf(String.format("importer.%s.table", dbType));
    }

    // First priority to system properties.
    public String getRequiredConf(String key)
    {
        if (Objects.nonNull(System.getProperty(key))) {
            return System.getProperty(key);
        }
        Objects.requireNonNull(appProps.getProperty(key), key + " is required. Please sampler.properties file.");
        return appProps.getProperty(key);
    }

    public String getConfWithDefault(String key, String defaultValue)
    {

        if (Objects.nonNull(System.getProperty(key))) {
            return System.getProperty(key);
        }

        return appProps.getProperty(key, defaultValue);
    }
}
