package com.axibase.tsd.driver.jdbc.connectionString;

import com.axibase.tsd.driver.jdbc.rules.OutputLogsToAllure;
import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
import io.qameta.allure.Step;
import io.qameta.allure.junit4.DisplayName;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static util.TestProperties.*;

public class CatalogTest {
    private static final String TABLE = "jvm_memory_used";
    private static final String QUERY = "SELECT datetime, time, value FROM jvm_memory_used LIMIT 1";

    @Rule
    public OutputLogsToAllure outputLogsToAllure = new OutputLogsToAllure(REDIRECT_OUTPUT_TO_ALLURE);

    private void runTestMetadataScenario(String connectionString, Matcher<Object> expectedCatalog) throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString, LOGIN_NAME, LOGIN_PASSWORD)) {
            testCatalogFromDbMetadataGetCatalogs(connection.getMetaData(), expectedCatalog);
            testCatalogFromDbMetadataGetColumns(connection.getMetaData(), expectedCatalog);
            testCatalogFromDbMetadataGetTables(connection.getMetaData(), expectedCatalog);
            try (final PreparedStatement preparedStatement = connection.prepareStatement(QUERY)) {
                testCatalogFromStatementMetadata(preparedStatement.getMetaData(), expectedCatalog);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    testCatalogFromResultSetMetadata(resultSet.getMetaData(), expectedCatalog);
                }
            }
        }
    }

    @Step("Test catalog from DatabaseMetadata#getCatalogs")
    private void testCatalogFromDbMetadataGetCatalogs(DatabaseMetaData metaData, Matcher<Object> expectedCatalog) throws SQLException {
        final ResultSet catalogs = metaData.getCatalogs();
        if (nullValue().equals(expectedCatalog)) {
            assertThat(catalogs.next(), is(false));
        } else {
            while (catalogs.next()) {
                assertThat(catalogs.getObject("TABLE_CAT"), is(expectedCatalog));
            }
        }
    }

    @Step("Test catalog from DatabaseMetadata#getColumns")
    private void testCatalogFromDbMetadataGetColumns(DatabaseMetaData metaData, Matcher<Object> expectedCatalog) throws SQLException {
        final ResultSet columns = metaData.getColumns(null, null, TABLE, null);
        while (columns.next()) {
            assertThat(columns.getObject("TABLE_CAT"), is(expectedCatalog));
        }
    }

    @Step("Test catalog from DatabaseMetadata#getTables")
    private void testCatalogFromDbMetadataGetTables(DatabaseMetaData metaData, Matcher<Object> expectedCatalog) throws SQLException {
        final ResultSet columns = metaData.getTables(null, null, TABLE, null);
        while (columns.next()) {
            assertThat(columns.getObject("TABLE_CAT"), is(expectedCatalog));
        }
    }

    private void testRsMetadataCatalog(ResultSetMetaData resultSetMetaData, Matcher<Object> expectedCatalog, boolean isCatalogSpecified) throws SQLException {
        final int columnCount = resultSetMetaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            if (isCatalogSpecified) {
                assertThat(resultSetMetaData.getCatalogName(i), is(expectedCatalog));
            } else {
                assertThat(resultSetMetaData.getCatalogName(i), is("")); // empty string returned if catalog not applicable
            }
        }
    }

    @Step("Test catalog from PreparedStatement#getMetaData")
    private void testCatalogFromStatementMetadata(ResultSetMetaData metaData,  Matcher<Object> expectedCatalogMatcher) throws SQLException {
        testRsMetadataCatalog(metaData, expectedCatalogMatcher, isCatalogExpected(expectedCatalogMatcher));
    }

    @Step("Test catalog from ResultSet#getMetaData")
    private void testCatalogFromResultSetMetadata(ResultSetMetaData metaData,  Matcher<Object> expectedCatalogMatcher) throws SQLException {
        testRsMetadataCatalog(metaData, expectedCatalogMatcher, isCatalogExpected(expectedCatalogMatcher));
    }

    @Test
    @DisplayName("Test that catalog is null by default")
    public void testWithoutCatalog() throws SQLException {
        runTestMetadataScenario(DEFAULT_JDBC_ATSD_URL, nullValue());
    }

    @Test
    @DisplayName("Test that catalog is applied to all metadata methods")
    public void testWithCatalog() throws SQLException {
        runTestMetadataScenario(getConnectStringWithCatalog("my_catalog"), equalTo("my_catalog"));
    }

    @Test
    @DisplayName("Test that empty catalog can be specified")
    public void testWithEmptyCatalog() throws SQLException {
        runTestMetadataScenario(getConnectStringWithCatalog(""), equalTo(""));
    }

    private static boolean isCatalogExpected(Matcher<Object> matcher) {
        return !matcher.matches(null);
    }

    private static String getConnectStringWithCatalog(String catalog) {
        return new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)
                .withCatalog(catalog)
                .composeConnectString();
    }
}
