package com.ibm.config;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ConfigTest
{
    @Test
    public void loadConfigTest()
            throws IOException
    {
        Config config = new Config();
        System.out.println(config.javaConfigPropertiesPath);
        config.loadViaJavaProperties();
        assertEquals("test", config.get("importer.source_schema"));
    }
}
