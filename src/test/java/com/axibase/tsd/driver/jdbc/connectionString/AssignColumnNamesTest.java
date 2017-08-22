package com.axibase.tsd.driver.jdbc.connectionString;

import com.axibase.tsd.driver.jdbc.DriverTestBase;
import com.axibase.tsd.driver.jdbc.rules.OutputLogsToAllure;
import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
import io.qameta.allure.Issue;
import io.qameta.allure.junit4.DisplayName;
import org.junit.Rule;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static util.TestProperties.*;

@Issue("4343")
public class AssignColumnNamesTest extends DriverTestBase {
    private static final String QUERY = "SELECT 'name' AS \"label\"";

    @Rule
    public OutputLogsToAllure outputLogsToAllure = new OutputLogsToAllure(REDIRECT_OUTPUT_TO_ALLURE);

    private void checkColumnNameAndLabel(String connectionString, String expectedName, String expectedLabel) throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString, LOGIN_NAME, LOGIN_PASSWORD);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(QUERY)) {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            assertThat(metaData.getColumnName(1), is(expectedName));
            assertThat(metaData.getColumnLabel(1), is(expectedLabel));
        }
    }

    @Test
    @DisplayName("Test that ResultSetMetaData#getColumnName returns column name instead of label if assignColumnName=true")
    public void testAssignTrue() throws SQLException {
        final String connString = getConnectStringWithAssignColumnNamesValue("true");
        checkColumnNameAndLabel(connString, "'name'", "label");
    }

    @Test
    @DisplayName("Test that ResultSetMetaData#getColumnName returns column label if assignColumnNames=false")
    public void testAssignFalse() throws SQLException {
        final String connString = getConnectStringWithAssignColumnNamesValue("false");
        checkColumnNameAndLabel(connString, "label", "label");
    }

    @Test
    @DisplayName("Test that ResultSetMetaData#getColumnName returns column label by default")
    public void testAssignDefault() throws SQLException {
        checkColumnNameAndLabel(DEFAULT_JDBC_ATSD_URL, "label", "label");
    }

    private static String getConnectStringWithAssignColumnNamesValue(String value) {
        return new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)
                .withAssignColumnNames(value)
                .composeConnectString();
    }

}
