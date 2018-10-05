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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static util.TestProperties.*;

@Slf4j
@Issue("4291")
public class ExpandTagsTest extends DriverTestBase {
    private static final String METRIC_WITH_TAGS = "java_method_invoke_average";
    private static final String[] AVAILABLE_TAGS = {"tags.\"host\"", "tags.\"name\""};
    private static final String TAGS_COLUMN = "tags";

    @Rule
    public OutputLogsToAllure outputLogsToAllure = new OutputLogsToAllure(REDIRECT_OUTPUT_TO_ALLURE);

    @Step("Retrieve column names from database metadata")
    private void checkColumns(String connectionString, Matcher<Iterable<String>> matcher) throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString, LOGIN_NAME, LOGIN_PASSWORD)) {
            final ResultSet columnsRs = connection.getMetaData().getColumns(null, null, METRIC_WITH_TAGS, null);
            Set<String> columns = new LinkedHashSet<>();
            while (columnsRs.next()) {
                final String tableName = columnsRs.getString("TABLE_NAME");
                if (!METRIC_WITH_TAGS.equals(tableName)) {
                    throw new IllegalStateException("Expected table name: '" + METRIC_WITH_TAGS + "', got: '" + tableName + "'");
                }
                columns.add(columnsRs.getString("COLUMN_NAME"));
            }
            log.info(columns.toString());
            assertThat(columns, matcher);
        }
    }

    @Test
    @DisplayName("Test that tags are expanded as additional columns if expandTags=true")
    public void testExpandTagsTrue() throws SQLException {
        final String connectionString = getConnectStringWithExpandTagsValue("true");
        checkColumns(connectionString, both(hasItems(AVAILABLE_TAGS)).and(hasItem(TAGS_COLUMN)));
    }

    @Test
    @DisplayName("Test that only tags column is exposed if expandTags=false")
    public void testExpandTagsFalse() throws SQLException {
        final String connectionString = getConnectStringWithExpandTagsValue("false");
        checkColumns(connectionString, both(not(hasItems(AVAILABLE_TAGS))).and(hasItem(TAGS_COLUMN)));
    }

    @Test
    @DisplayName("Test that default behavior is not exposing tags as separate columns")
    public void testExpandTagsDefault() throws SQLException {
        checkColumns(DEFAULT_JDBC_ATSD_URL, both(not(hasItems(AVAILABLE_TAGS))).and(hasItem(TAGS_COLUMN)));
    }

    private static String getConnectStringWithExpandTagsValue(String value) {
        return new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)
                .withExpandTags(value)
                .composeConnectString();
    }

}
