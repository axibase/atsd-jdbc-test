package com.axibase.tsd.driver.jdbc.data;

import com.axibase.tsd.driver.jdbc.DriverConstants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static util.TestProperties.DEFAULT_JDBC_ATSD_URL;
import static util.TestProperties.LOGIN_NAME;
import static util.TestProperties.LOGIN_PASSWORD;

public class AtsdConnectionTest extends AbstractDataTest {
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
