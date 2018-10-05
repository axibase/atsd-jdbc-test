package com.axibase.tsd.driver.jdbc.data;

import com.axibase.tsd.driver.jdbc.AtsdDriver;
import com.axibase.tsd.driver.jdbc.DriverConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static util.TestProperties.*;

public class AtsdConnectionTest extends AbstractDataTest {
    private static AtsdDriver driver;

    @BeforeClass
    public static void setUpBeforeClass() {
        driver = new AtsdDriver();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        DriverManager.deregisterDriver(driver);
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testConnectStringProperties() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", LOGIN_NAME);
        properties.setProperty("password", LOGIN_PASSWORD);
        Connection connection = driver.connect(DEFAULT_JDBC_ATSD_URL, properties);
        assertNotNull(connection);
        connection.close();
    }

    @Test
    public void testConnectWithoutCredentials() throws Exception {
        exception.expect(SQLException.class);
        exception.expectMessage("Wrong credentials provided");
        Connection connection = driver.connect(DEFAULT_JDBC_ATSD_URL, new Properties());
        connection.close();
    }

    @Test
    public void testConnectToWrongUrl() throws Exception {
        exception.expect(SQLException.class);
        exception.expectMessage("Unknown host specified");
        Connection connection = driver.connect(DriverConstants.CONNECT_URL_PREFIX + "unknown:443", new Properties());
        connection.close();
    }
}
