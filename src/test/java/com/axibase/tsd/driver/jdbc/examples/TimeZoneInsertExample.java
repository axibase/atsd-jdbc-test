package com.axibase.tsd.driver.jdbc.examples;

import io.qameta.allure.Step;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static util.TestProperties.LOGIN_NAME;
import static util.TestProperties.LOGIN_PASSWORD;

public class TimeZoneInsertExample {
    private static final String ENTITY = "test-tz";
    private static final String METRIC = "m-insert-dt";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of("UTC"));


    private final TimeZone originalTimeZone = TimeZone.getDefault();

    @Before
    public void setTimeZone() {
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"));
    }

    @After
    public void revertTimeZone() {
        TimeZone.setDefault(originalTimeZone);
    }

    @Test
    public void testTimeZones() throws SQLException {
        System.out.println(TimeZone.getDefault());
        final String timestampStr = "2017-08-22 00:00:00";

        try (final Connection connection = DriverManager.getConnection("jdbc:atsd://localhost:8443;timestamptz=true", LOGIN_NAME, LOGIN_PASSWORD)) {
            fillData(connection, timestampStr, "true");
        }
        try (final Connection connection = DriverManager.getConnection("jdbc:atsd://localhost:8443;timestamptz=false;", LOGIN_NAME, LOGIN_PASSWORD)) {
            fillData(connection, timestampStr, "false");
        }
    }

    @Step("Insert data into m-insert-dt-string and m-insert-dt-timestamp with tag timestampTz}")
    private static void fillData(Connection connection, String timeStamp, String timestamptzTag) throws SQLException {
        final String pattern = "INSERT INTO \"%s\" (datetime, entity, value, tags.timestamptz, tags.setter) VALUES (?,?,?,?,?)";
        try (PreparedStatement stmt = connection.prepareStatement(String.format(pattern, METRIC))) {
            stmt.setString(1, timeStamp);
            stmt.setString(2, ENTITY);
            stmt.setDouble(3, 0);
            stmt.setString(4, timestamptzTag);
            stmt.setString(5, "setString");
            assertThat(stmt.executeUpdate(), is(1));

            final long millis = ZonedDateTime.parse(timeStamp, FORMATTER).toInstant().toEpochMilli();
            stmt.setTimestamp(1, new Timestamp(millis));
            stmt.setString(2, ENTITY);
            stmt.setDouble(3, 0);
            stmt.setString(4, timestamptzTag);
            stmt.setString(5, "setTimestamp");
            assertThat(stmt.executeUpdate(), is(1));

            stmt.setLong(1, millis);
            stmt.setString(2, ENTITY);
            stmt.setDouble(3, 0);
            stmt.setString(4, timestamptzTag);
            stmt.setString(5, "setLong");
            assertThat(stmt.executeUpdate(), is(1));
        }
    }
}
