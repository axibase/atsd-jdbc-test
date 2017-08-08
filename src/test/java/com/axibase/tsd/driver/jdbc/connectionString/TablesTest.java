package com.axibase.tsd.driver.jdbc.connectionString;

import com.axibase.tsd.driver.jdbc.DriverTestBase;
import com.axibase.tsd.driver.jdbc.rules.OutputLogsToAllure;
import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
import io.qameta.allure.Issue;
import io.qameta.allure.Step;
import io.qameta.allure.junit4.DisplayName;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static util.TestProperties.*;

@Slf4j
public class TablesTest extends DriverTestBase {
    @Rule
    public final OutputLogsToAllure outputLogsToAllure = new OutputLogsToAllure(REDIRECT_OUTPUT_TO_ALLURE);

    @Step("Retrieve tables using DatabaseMetadata#getTables method")
    private Set<String> testGetTablesSet(String connectionString, DatabaseMetaData metaData, Matcher<Integer> sizeMatcher, Matcher<Iterable<? extends String>> tablesMatcher) throws SQLException {
        log.info("Connection String: {}", connectionString);
        final Set<String> getTablesSet = getTablesSet(metaData.getTables(null, null, null, null));
        assertThat(getTablesSet, hasSize(sizeMatcher));
        assertThat(getTablesSet, tablesMatcher);
        log.info("Number of tables: {}", getTablesSet.size());
        return getTablesSet;
    }

    @Step("Retrieve tables using DatabaseMetadata#getColumns method")
    private Set<String> testGetColumnsSet(DatabaseMetaData metaData, Matcher<Integer> sizeMatcher, Matcher<Iterable<? extends String>> tablesMatcher, String connectionString) throws SQLException {
        log.info("Connection String: {}", connectionString);
        final Set<String> getColumnsSet = getTablesSet(metaData.getColumns(null, null, null, null));
        assertThat(getColumnsSet, hasSize(sizeMatcher));
        assertThat(getColumnsSet, tablesMatcher);
        log.info("Number of tables: {}", getColumnsSet.size());
        return getColumnsSet;
    }

    @Step("Test that getTables and getColumns contain same values")
    private void testSetsEquality(Set<String> getTablesSet, Set<String> getColumnsSet) throws SQLException {
        assertThat(getColumnsSet, containsInAnyOrder(getTablesSet.toArray(new String[0])));
        assertThat(getColumnsSet, hasSize(getTablesSet.size()));
    }

    private void getTablesGetColumnsCompare(String connectionString, Matcher<Integer> sizeMatcher, Matcher<Iterable<? extends String>> tablesMatcher) throws SQLException {
        try (final Connection connection = DriverManager.getConnection(connectionString, LOGIN_NAME, LOGIN_PASSWORD)) {
            final DatabaseMetaData metaData = connection.getMetaData();
            final Set<String> tablesFromTables = testGetTablesSet(connectionString, metaData, sizeMatcher, tablesMatcher);
            final Set<String> tablesFromColumns = testGetColumnsSet(metaData, sizeMatcher, tablesMatcher, connectionString);
            testSetsEquality(tablesFromTables, tablesFromColumns);
        }
    }

    @Test
    @DisplayName("Test that all metrics are represented as tables by default")
    public void testDefaultValue() throws SQLException {
        getTablesGetColumnsCompare(DEFAULT_JDBC_ATSD_URL, greaterThan(0), not(contains("atsd_series")));
    }

    @Test
    @Issue("4383")
    @DisplayName("Test that 'match all' wildcard allows to represent all metrics as tables together with 'atsd_series'")
    public void testAllMetrics() throws SQLException {
        final String connectionString = getConnectStringWithTables("*");
        getTablesGetColumnsCompare(connectionString, greaterThan(1), contains("atsd_series"));
    }

    @Test
    @DisplayName("Test that atsd_series can be resolved from string")
    public void testAtsdSeriesRaw() throws SQLException {
        final String connectionString = getConnectStringWithTables("atsd_series");
        getTablesGetColumnsCompare(connectionString, equalTo(1), contains("atsd_series"));
    }

    @Test
    @Issue("4383")
    @DisplayName("Test that atsd_series can be resolved from wildcard")
    public void testAtsdSeriesWildcard() throws SQLException {
        final String connectionString = getConnectStringWithTables("atsd?series");
        getTablesGetColumnsCompare(connectionString, equalTo(1), contains("atsd_series"));
    }

    @Test
    @DisplayName("Test that explicitly specified non-existent metrics are added to list of tables")
    public void testNonExistentMetricsShownOnGetTables() throws SQLException {
        final String connectionString = getConnectStringWithTables("tablestest_nonexistent_metric");
        getTablesGetColumnsCompare(connectionString, equalTo(1), contains("tablestest_nonexistent_metric"));
    }

    private static Set<String> getTablesSet(ResultSet tablesResultSet) throws SQLException {
        Set<String> tables = new HashSet<>();
        while (tablesResultSet.next()) {
            tables.add(tablesResultSet.getString("TABLE_NAME"));
        }
        return tables;
    }

    private static String getConnectStringWithTables(String tables) {
        return new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)
                .withTables(tables)
                .composeConnectString();
    }
}
