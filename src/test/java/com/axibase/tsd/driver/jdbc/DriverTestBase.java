package com.axibase.tsd.driver.jdbc;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.DriverManager;

public class DriverTestBase {
    protected static AtsdDriver driver;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        driver = new AtsdDriver();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        DriverManager.deregisterDriver(driver);
    }

}