package com.axibase.tsd.driver.jdbc.connectionString;

import com.axibase.tsd.driver.jdbc.DriverTestBase;
import com.axibase.tsd.driver.jdbc.rules.OutputLogsToAllure;
import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
import io.qameta.allure.Issue;
import io.qameta.allure.Step;
import io.qameta.allure.junit4.DisplayName;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static util.TestProperties.*;

@Issue("4346")
public class MetaColumnsTest extends DriverTestBase {
    private static final String TABLE = "jvm_memory_used";
    private static final String[] META_COLUMNS = {"entity.enabled", "entity.groups", "entity.interpolate", "entity.label",
            "entity.tags", "entity.timeZone", "metric.dataType", "metric.description", "metric.enabled", "metric.filter",
            "metric.interpolate", "metric.invalidValueAction", "metric.label", "metric.lastInsertTime", "metric.maxValue",
            "metric.minValue", "metric.name", "metric.persistent", "metric.retentionIntervalDays", "metric.tags",
            "metric.timePrecision", "metric.timeZone", "metric.versioning", "metric.units"};

    @Rule
    public OutputLogsToAllure outputLogsToAllure = new OutputLogsToAllure(REDIRECT_OUTPUT_TO_ALLURE);

    private void metaColumnsTestScenario(String connectionString, Matcher<Iterable<String>> columnsMatcher, String expectedRemarks) throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString, LOGIN_NAME, LOGIN_PASSWORD)) {
            final DatabaseMetaData metaData = connection.getMetaData();
            testColumns(metaData, columnsMatcher);
            testTableRemark(metaData, expectedRemarks);
        }
    }

    @Step
    private void testColumns(DatabaseMetaData metaData, Matcher<Iterable<String>> matches) throws SQLException {
        final ResultSet columns = metaData.getColumns(null, null, TABLE, null);
        final Set<String> columnNames = new HashSet<>();
        while (columns.next()) {
            final String tableName = columns.getString("TABLE_NAME");
            if (!TABLE.equals(tableName)) {
                throw new IllegalStateException("Expected table '" + TABLE + "', but got '" + tableName);
            }
            columnNames.add(columns.getString("COLUMN_NAME"));
        }
        assertThat(columnNames, matches);
    }

    @Step
    private void testTableRemark(DatabaseMetaData metaData, String expectedRemarks) throws SQLException {
        final ResultSet columns = metaData.getTables(null, null, TABLE, null);
        while (columns.next()) {
            final String tableName = columns.getString("TABLE_NAME");
            if (!TABLE.equals(tableName)) {
                throw new IllegalStateException("Expected table '" + TABLE + "', but got '" + tableName);
            }
            assertThat(columns.getString("REMARKS"), is(expectedRemarks));
        }
    }

    @Test
    @DisplayName("Test that meta columns are exposed in getColumns and table remarks if metaColumns=true")
    public void testMetaColumnsTrue() throws SQLException {
        final String connectString = getConnectStringWithMetaColumnsValue("true");
        final String expectedRemarks = "SELECT time, datetime, value, text, metric, entity, tags, " +
                "entity.enabled, entity.groups, entity.interpolate, entity.label, entity.tags, entity.timeZone, " +
                "metric.dataType, metric.description, metric.enabled, metric.filter, metric.interpolate, " +
                "metric.invalidValueAction, metric.label, metric.lastInsertTime, metric.maxValue, metric.minValue, " +
                "metric.name, metric.persistent, metric.retentionIntervalDays, metric.tags, metric.timePrecision, " +
                "metric.timeZone, metric.versioning, metric.units FROM \"" + TABLE + "\" LIMIT 1";
        metaColumnsTestScenario(connectString, hasItems(META_COLUMNS), expectedRemarks);
    }

    @Test
    @DisplayName("Test that meta columns are not exposed in getColumns and table remarks if metaColumns=false")
    public void testMetaColumnsFalse() throws SQLException {
        final String connectString = getConnectStringWithMetaColumnsValue("false");
        final String expectedRemarks = "SELECT time, datetime, value, text, metric, entity, tags FROM \"" + TABLE + "\" LIMIT 1";
        metaColumnsTestScenario(connectString, not(hasItems(META_COLUMNS)), expectedRemarks);
    }

    @Test
    @DisplayName("Test that meta columns are hidden by default")
    public void testMetaColumnsDefault() throws SQLException {
        final String expectedRemarks = "SELECT time, datetime, value, text, metric, entity, tags FROM \"" + TABLE + "\" LIMIT 1";
        metaColumnsTestScenario(DEFAULT_JDBC_ATSD_URL, not(hasItems(META_COLUMNS)), expectedRemarks);
    }

    private static String getConnectStringWithMetaColumnsValue(String value) {
        return new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)
                .withMetaColumns(value)
                .composeConnectString();
    }
}
