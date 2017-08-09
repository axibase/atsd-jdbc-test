package com.axibase.tsd.driver.jdbc.connectionString;

import com.axibase.tsd.driver.jdbc.rules.OutputLogsToAllure;
import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
import io.qameta.allure.Step;
import io.qameta.allure.junit4.DisplayName;
import org.junit.Rule;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static util.TestProperties.*;

public class CompatibilityTest {
    private static final String TABLE = "jvm_memory_used";
    private static final String QUERY = "SELECT datetime, time, value FROM jvm_memory_used LIMIT 1";
    private static final int ODBC_TIMESTAMP_TYPE = 11;

    @Rule
    public OutputLogsToAllure outputLogsToAllure = new OutputLogsToAllure(REDIRECT_OUTPUT_TO_ALLURE);

    private void runTestMetadataScenario(String connectionString, Map<String, Integer> expectedTypes) throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString, LOGIN_NAME, LOGIN_PASSWORD)) {
            testColumnsDatatypesFromDbMetadata(connection.getMetaData(), expectedTypes);
            try (final PreparedStatement preparedStatement = connection.prepareStatement(QUERY)) {
                testColumnsDatatypesFromStatementMetadata(preparedStatement.getMetaData(), expectedTypes);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    testColumnsDatatypesFromResultSetMetadata(resultSet.getMetaData(), expectedTypes);
                }
            }
        }
    }

    @Step("Test column datatypes from DatabaseMetadata#getColumns")
    private void testColumnsDatatypesFromDbMetadata(DatabaseMetaData metaData, Map<String, Integer> expectedTypes) throws SQLException {
        final ResultSet columns = metaData.getColumns(null, null, TABLE, null);
        int assertions = 0;
        while (columns.next()) {
            final String columnName = columns.getString("COLUMN_NAME");
            final Integer expectedType = expectedTypes.get(columnName);
            if (expectedType != null) {
                assertThat("Unexpected type for column " + columnName, columns.getInt("DATA_TYPE"), is(expectedType));
                ++assertions;
            }
        }
        if (assertions < expectedTypes.size()) {
            throw new IllegalStateException("Not all expected column names found");
        }
    }

    private void testRsMetadataTypes(ResultSetMetaData resultSetMetaData, Map<String, Integer> expectedTypes) throws SQLException {
        final int columnCount = resultSetMetaData.getColumnCount();
        int assertions = 0;
        for (int i = 1; i <= columnCount; i++) {
            final String columnLabel = resultSetMetaData.getColumnLabel(i);
            final Integer expectedType = expectedTypes.get(columnLabel);
            if (expectedType != null) {
                assertThat("Unexpected type for column " + columnLabel, resultSetMetaData.getColumnType(i), is(expectedType));
                ++assertions;
            }
        }
        if (assertions < expectedTypes.size()) {
            throw new IllegalStateException("Not all expected column names found");
        }
    }

    @Step("Test column datatypes from PreparedStatement#getMetaData")
    private void testColumnsDatatypesFromStatementMetadata(ResultSetMetaData metaData, Map<String, Integer> expectedTypes) throws SQLException {
        testRsMetadataTypes(metaData, expectedTypes);
    }

    @Step("Test column datatypes from ResultSet#getMetaData")
    private void testColumnsDatatypesFromResultSetMetadata(ResultSetMetaData metaData, Map<String, Integer> expectedTypes) throws SQLException {
        testRsMetadataTypes(metaData, expectedTypes);
    }

    @Test
    @DisplayName("Test metadata in normal mode")
    public void testCompatibilityModeDisabled() throws SQLException {
        final Map<String, Integer> expectedTypes = new HashMap<>(2);
        expectedTypes.put("time", Types.BIGINT);
        expectedTypes.put("datetime", Types.TIMESTAMP);
        runTestMetadataScenario(DEFAULT_JDBC_ATSD_URL, expectedTypes);
    }

    @Test
    @DisplayName("Test odbc2 compatibility mode")
    public void testOdbcCompatibility() throws SQLException {
        final Map<String, Integer> expectedTypes = new HashMap<>(2);
        expectedTypes.put("time", Types.DOUBLE);
        expectedTypes.put("datetime", ODBC_TIMESTAMP_TYPE);
        runTestMetadataScenario(getConnectStringWithCompatibility("odbc2"), expectedTypes);
    }

    private static String getConnectStringWithCompatibility(String compatibility) {
        return new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)
                .withCompatibility(compatibility)
                .composeConnectString();
    }
}
