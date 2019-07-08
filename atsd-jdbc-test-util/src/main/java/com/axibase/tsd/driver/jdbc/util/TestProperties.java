package com.axibase.tsd.driver.jdbc.util;


import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@UtilityClass
@Slf4j
public final class TestProperties {
    static {
        fillSystemPropertiesFromDevPropertiesFile();
    }

    // Table names used in conditional tests
    public static final String TINY_TABLE_COUNT_KEY = "axibase.tsd.driver.jdbc.metric.tiny.count";
    public static final String TINY_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.tiny";
    public static final String SMALL_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.small";
    public static final String MEDIUM_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.medium";
    public static final String LARGE_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.large";
    public static final String HUGE_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.huge";
    public static final String JUMBO_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.jumbo";
    public static final String TWO_TABLES_KEY = "axibase.tsd.driver.jdbc.metric.concurrent";
    public static final String WRONG_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.wrong";

    // Table name values
    public static final long TINY_TABLE_COUNT = getCount(System.getProperty(TINY_TABLE_COUNT_KEY));
    public static final String TINY_TABLE = System.getProperty(TINY_TABLE_KEY);
    public static final String SMALL_TABLE = System.getProperty(SMALL_TABLE_KEY);
    public static final String MEDIUM_TABLE = System.getProperty(MEDIUM_TABLE_KEY);
    public static final String LARGE_TABLE = System.getProperty(LARGE_TABLE_KEY);
    public static final String HUGE_TABLE = System.getProperty(HUGE_TABLE_KEY);
    public static final String JUMBO_TABLE = System.getProperty(JUMBO_TABLE_KEY);
    public static final String TWO_TABLES = System.getProperty(TWO_TABLES_KEY);
    public static final String WRONG_TABLE = System.getProperty(WRONG_TABLE_KEY);

    public static final String HTTP_ATSD_URL = System.getProperty("axibase.tsd.driver.jdbc.url");
    public static final String LOGIN_NAME = System.getProperty("axibase.tsd.driver.jdbc.username");
    public static final String LOGIN_PASSWORD = System.getProperty("axibase.tsd.driver.jdbc.password");
    public static final String READ_STRATEGY = System.getProperty("axibase.tsd.driver.jdbc.strategy");
    public static final boolean REDIRECT_OUTPUT_TO_ALLURE = "true".equalsIgnoreCase(System.getProperty("output.redirect.allure"));
    public static final long INSERT_WAIT = getInsertWait(System.getProperty("insert.wait"));

    public static final String DEFAULT_JDBC_ATSD_URL;
    public static final AtsdConnectionInfo DEFAULT_CONNECTION_INFO;
    static {
        final ConnectStringComposer composer = new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD);
        DEFAULT_JDBC_ATSD_URL = composer.composeConnectString();
        DEFAULT_CONNECTION_INFO = composer.composeConnectionInfo();

        log.info("tiny table: " + TestProperties.TINY_TABLE + ", count = " + TestProperties.TINY_TABLE_COUNT);
        log.info("small table: " + TestProperties.SMALL_TABLE);
        log.info("medium table: " + TestProperties.MEDIUM_TABLE);
        log.info("large table: " + TestProperties.LARGE_TABLE);
        log.info("huge table: " + TestProperties.HUGE_TABLE);
    }

    private static void fillSystemPropertiesFromDevPropertiesFile() {
        final InputStream devProperties = TestProperties.class.getClassLoader().getResourceAsStream("dev.properties");
        if (devProperties == null) {
            throw new IllegalStateException("dev.properties not found");
        }
        final Properties properties = new Properties();
        try {
            properties.load(devProperties);
        } catch (IOException e) {
            throw new IllegalStateException("Could not load properties from dev.properties: " + e.getMessage(), e);
        }
        final Properties systemProperties = System.getProperties();
        properties.forEach(systemProperties::putIfAbsent);
    }

    private static long getInsertWait(String property) {
        if (property != null) {
            try {
                return Long.parseLong(property);
            } catch (NumberFormatException e) {
                log.error("Error while setting insert.wait parameter: {}", property, e);
            }
        }
        return 1000L;
    }

    private static long getCount(String countVar) {
        return !StringUtils.isEmpty(countVar) ? Long.parseLong(countVar) : -1L;
    }
}
