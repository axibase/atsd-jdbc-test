package com.axibase.tsd.driver.jdbc.connectionString;

import com.axibase.tsd.driver.jdbc.rules.OutputLogsToAllure;
import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
import io.qameta.allure.Flaky;
import io.qameta.allure.Issue;
import io.qameta.allure.junit4.DisplayName;
import lombok.AllArgsConstructor;
import org.junit.*;
import util.TestProperties;

import java.sql.*;
import java.time.Instant;
import java.util.TimeZone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static util.TestProperties.*;

@Issue("4369")
public class TimestampTzTest {
    private static final long TIME = Instant.parse("2017-08-01T00:00:00Z").toEpochMilli();

    private static TimeZone defaultTimeZone;
    private static int timeZoneOffset;

    @Rule
    public OutputLogsToAllure outputLogsToAllure = new OutputLogsToAllure(REDIRECT_OUTPUT_TO_ALLURE);

    @BeforeClass
    public static void setTimeZone() {
        defaultTimeZone = TimeZone.getDefault();
        final TimeZone newTimeZone = TimeZone.getTimeZone("Europe/Berlin");
        TimeZone.setDefault(newTimeZone);
        timeZoneOffset = newTimeZone.getOffset(TIME);
    }

    @AfterClass
    public static void revertTimeZone() {
        TimeZone.setDefault(defaultTimeZone);
    }

    @Test
    @DisplayName("Test that timestamps are manipulated in select statements assuming that getTime() returns ms since epoch")
    public void timestampTzIsEnabledInSelect() throws SQLException {
        final String connString = getConnectStringWithTimestampTzValue("true");
        final long setTime = testTimestampTzInSelect(connString, new Timestamp(TIME));
        assertThat(setTime, is(TIME));
    }

    @Test
    @DisplayName("Test that timestamps are manipulated in select statements assuming that getTime() returns ms since 1970-01-01 in local time")
    public void timestampTzIsDisabledInSelect() throws SQLException {
        final String connString = getConnectStringWithTimestampTzValue("false");
        final long setTime = testTimestampTzInSelect(connString, new Timestamp(TIME));
        assertThat(setTime, is(TIME + timeZoneOffset));
    }

    @Test
    @DisplayName("Test that timestamptz is true by default in select statements")
    public void timestampTzIsDefaultInSelect() throws SQLException {
        final long setTime = testTimestampTzInSelect(DEFAULT_JDBC_ATSD_URL, new Timestamp(TIME));
        assertThat(setTime, is(TIME));
    }

    @Test
    @Flaky
    @DisplayName("Test that timestamps are manipulated in insert statements assuming that getTime() returns ms since epoch")
    public void timestampTzIsEnabledInInsert() throws SQLException {
        final String connString = getConnectStringWithTimestampTzValue("true");
        final TimeAndDatetime setTimeAndDatetime = testTimestampTzInInsert(connString, new Timestamp(TIME), "timestamptz_true");
        assertThat(setTimeAndDatetime.time, is(TIME));
        assertThat(setTimeAndDatetime.datetime.getTime(), is(TIME));
    }

    @Test
    @Flaky
    @DisplayName("Test that timestamps are manipulated in insert statements assuming that getTime() returns ms since 1970-01-01 in local time")
    public void timestampTzIsDisabledInInsert() throws SQLException {
        final String connString = getConnectStringWithTimestampTzValue("false");
        final TimeAndDatetime setTimeAndDatetime = testTimestampTzInInsert(connString, new Timestamp(TIME), "timestamptz_false");
        assertThat(setTimeAndDatetime.time, is(TIME + timeZoneOffset));
        assertThat(setTimeAndDatetime.datetime.getTime(), is(TIME));
    }

    @Test
    @Flaky
    @DisplayName("Test that timestamptz is true by default in insert statements")
    public void timestampTzIsDefaultInInsert() throws SQLException {
        final TimeAndDatetime setTimeAndDatetime = testTimestampTzInInsert(DEFAULT_JDBC_ATSD_URL, new Timestamp(TIME), "timestamptz_default");
        assertThat(setTimeAndDatetime.time, is(TIME));
        assertThat(setTimeAndDatetime.datetime.getTime(), is(TIME));
    }

    private long testTimestampTzInSelect(String connString, Timestamp timestamp) throws SQLException {
        final String sql = "SELECT date_parse(?)";
        try (Connection connection = DriverManager.getConnection(connString, LOGIN_NAME, LOGIN_PASSWORD);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, timestamp);
            final ResultSet resultSet = statement.executeQuery();
            assertThat(resultSet.next(), is(true));
            return resultSet.getLong(1);
        }
    }

    private TimeAndDatetime testTimestampTzInInsert(String connString, Timestamp timestamp, String entity) throws SQLException {
        final String sqlInsert = "INSERT INTO test_timestamptz (datetime, entity, value) VALUES(?, ?, 42)";
        final String sqlSelect = "SELECT time, datetime FROM test_timestamptz WHERE entity = ? ORDER BY time DESC LIMIT 1";
        try (Connection connection = DriverManager.getConnection(connString, LOGIN_NAME, LOGIN_PASSWORD);
             PreparedStatement insertStatement = connection.prepareStatement(sqlInsert)) {
            insertStatement.setTimestamp(1, timestamp);
            insertStatement.setString(2, entity);
            int inserted = insertStatement.executeUpdate();
            assertThat(inserted, is (1));

            try {
                Thread.sleep(TestProperties.INSERT_WAIT);
            } catch (InterruptedException e) {
                Assert.fail(e.getMessage());
            }
            try (PreparedStatement selectStatement = connection.prepareStatement(sqlSelect)) {
                selectStatement.setString(1, entity);
                ResultSet resultSet = selectStatement.executeQuery();
                assertThat(resultSet.next(), is(true));
                return new TimeAndDatetime(resultSet.getLong("time"), resultSet.getTimestamp("datetime"));
            }
        }
    }

    private static String getConnectStringWithTimestampTzValue(String value) {
        return new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)
                .withTimestamptz(value)
                .composeConnectString();
    }

    @AllArgsConstructor
    private static final class TimeAndDatetime {
        private final long time;
        private final Timestamp datetime;
    }
}
