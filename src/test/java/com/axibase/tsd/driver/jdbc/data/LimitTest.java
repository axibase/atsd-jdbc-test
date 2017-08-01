package com.axibase.tsd.driver.jdbc.data;


import com.axibase.tsd.driver.jdbc.DriverTestBase;
import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import com.axibase.tsd.driver.jdbc.rules.ExecuteWhenSysVariableSet;

import java.sql.*;

import static com.axibase.tsd.driver.jdbc.TestConstants.SELECT_ALL_CLAUSE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static util.TableConstants.*;
import static util.TestProperties.*;

@Slf4j
@RunWith(DataProviderRunner.class)
public class LimitTest extends DriverTestBase {

    private Connection connection;

    @Before
    public void setUp() throws SQLException {
        connection = DriverManager.getConnection(DEFAULT_JDBC_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD);
    }

    @After
    public void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @DataProvider
    public static Object[][] dataQueryLimitMaxRowsResult() {
        return new Object[][] {
                { 5, 3, 3 },
                { 5, 10, 5 },
                { 5, null, 5 },
                { 0, 3, 3 },
                { null, 3, 3 }
        };
    }

    @Test
    @ExecuteWhenSysVariableSet(TINY_TABLE_KEY)
    @UseDataProvider("dataQueryLimitMaxRowsResult")
    public void testPreparedStatementLimit(Integer maxRows, Integer queryLimit, long expectedResultsetSize) throws SQLException, AtsdException {
        StringBuilder sql = new StringBuilder(SELECT_ALL_CLAUSE).append(TINY_TABLE);
        if (queryLimit != null) {
            sql.append(" LIMIT ").append(queryLimit);
        }
        try (final PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            if (maxRows != null) {
                statement.setMaxRows(maxRows);
            }
            try (final ResultSet resultSet = statement.executeQuery()) {
                assertThat(getResultSetSize(resultSet), is(expectedResultsetSize));
            }
        }
    }


    @Test
    @ExecuteWhenSysVariableSet(TINY_TABLE_KEY)
    @UseDataProvider("dataQueryLimitMaxRowsResult")
    public void testStatementLimit(Integer maxRows, Integer queryLimit, long expectedResultsetSize) throws SQLException, AtsdException {
        StringBuilder sql = new StringBuilder(SELECT_ALL_CLAUSE).append(TINY_TABLE);
        if (queryLimit != null) {
            sql.append(" LIMIT ").append(queryLimit);
        }
        try (final Statement statement = connection.createStatement()) {
            if (maxRows != null) {
                statement.setMaxRows(maxRows);
            }
            try (final ResultSet resultSet = statement.executeQuery(sql.toString())) {
                assertThat(getResultSetSize(resultSet), is(expectedResultsetSize));
            }
        }
    }

    @Test
    @ExecuteWhenSysVariableSet(TINY_TABLE_KEY)
    @ExecuteWhenSysVariableSet(TINY_TABLE_COUNT_KEY)
    public void testStatementWithoutLimits() throws SQLException, AtsdException {
        final String sql = SELECT_ALL_CLAUSE + TINY_TABLE;
        try (final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery(sql)) {

        assertThat(getResultSetSize(resultSet), is(TINY_TABLE_COUNT));
        }
    }

    @Test
    @ExecuteWhenSysVariableSet(TINY_TABLE_KEY)
    @ExecuteWhenSysVariableSet(TINY_TABLE_COUNT_KEY)
    public void testPreparedStatementWithoutLimits() throws SQLException, AtsdException {
        final String sql = SELECT_ALL_CLAUSE + TINY_TABLE;
        try (final PreparedStatement statement = connection.prepareStatement(sql);
             final ResultSet resultSet = statement.executeQuery()) {

            assertThat(getResultSetSize(resultSet), is(TINY_TABLE_COUNT));

        }
    }

    private static long getResultSetSize(ResultSet resultSet) throws SQLException {
        long resultSetSize = 0L;
        while(resultSet.next()) {
            ++resultSetSize;
        }
        return resultSetSize;
    }

}
