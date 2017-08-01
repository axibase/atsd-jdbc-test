package com.axibase.tsd.driver.jdbc.connectionString;

import com.axibase.tsd.driver.jdbc.DriverTestBase;
import com.axibase.tsd.driver.jdbc.ext.AtsdMetricNotFoundException;
import com.axibase.tsd.driver.jdbc.rules.OutputLogsToAllure;
import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
import io.qameta.allure.Issue;
import io.qameta.allure.junit4.DisplayName;
import org.junit.Rule;
import org.junit.Test;
import util.TableConstants;

import java.sql.*;

import static com.axibase.tsd.driver.jdbc.TestConstants.SELECT_ALL_CLAUSE;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static util.TestProperties.*;

@Issue("4385")
public class MissingMetricTest extends DriverTestBase {
    @Rule
    public OutputLogsToAllure outputLogsToAllure = new OutputLogsToAllure(REDIRECT_OUTPUT_TO_ALLURE);

    @Test
    @DisplayName("Test that warning appears if missingMetric=warning is set and metric doesn't exist")
    public void warnOnMissingMetric() throws SQLException {
        final String connString = getConnectStringWithMissingMetricValue("warning");
        try (Connection connection = DriverManager.getConnection(connString, LOGIN_NAME, LOGIN_PASSWORD);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SELECT_ALL_CLAUSE + TableConstants.WRONG_TABLE);) {
            assertThat(resultSet.getWarnings(), is(not(nullValue())));
        }
    }

    @Test(expected = AtsdMetricNotFoundException.class)
    @DisplayName("Test that exception is thrown if missingMetric=error is set and metric doesn't exist")
    public void errorOnMissingMetric() throws SQLException {
        final String connString = getConnectStringWithMissingMetricValue("error");
        try (Connection connection = DriverManager.getConnection(connString, LOGIN_NAME, LOGIN_PASSWORD);
             Statement statement = connection.createStatement()) {
            statement.executeQuery(SELECT_ALL_CLAUSE + TableConstants.WRONG_TABLE);
        }
    }

    @Test
    @DisplayName("Test that empty result set is returned if missingMetric=none is set and metric doesn't exist")
    public void noneOnMissingMetric() throws SQLException {
        final String connString = getConnectStringWithMissingMetricValue("none");
        try (Connection connection = DriverManager.getConnection(connString, LOGIN_NAME, LOGIN_PASSWORD);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SELECT_ALL_CLAUSE + TableConstants.WRONG_TABLE);) {
            assertThat(resultSet, is(not(nullValue())));
            assertThat(resultSet.next(), is(false));
        }
    }

    @Test
    @DisplayName("Test that default behavior is similar to missingMetric=warning")
    public void warnOnMissingMetricDefault() throws SQLException {
        try (Connection connection = DriverManager.getConnection(DEFAULT_JDBC_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SELECT_ALL_CLAUSE + TableConstants.WRONG_TABLE);) {
            assertThat(resultSet.getWarnings(), is(not(nullValue())));
        }
    }

    private static String getConnectStringWithMissingMetricValue(String value) {
        return new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)
                .withMissingMetric(value)
                .composeConnectString();
    }
}
