package com.axibase.tsd.driver.jdbc.data;

import io.qameta.allure.junit4.DisplayName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class CommentsTest extends AbstractDataTest {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    @DisplayName("Test that select statement starting with comment is sent to ATSD")
    public void testSelectStatement() throws SQLException {
        String sql = "-- ping query\n SELECT 1";
        try (final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(sql)) {
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt(1), is(1));
        }
    }

    @Test
    @DisplayName("Test that prepared select statement starting with comment is sent to ATSD")
    public void testSelectPreparedStatement() throws SQLException {
        String sql = "-- ping query\n SELECT 1";
        try (final PreparedStatement statement = connection.prepareStatement(sql);
             final ResultSet resultSet = statement.executeQuery()) {
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt(1), is(1));
        }
    }

    @Test
    @DisplayName("Test that insert statement starting with comment is executed")
    public void testInsertStatement() throws SQLException {
        String sql = "-- some insert query\n INSERT INTO blackhole (entity, time, value) VALUES ('hole', 0, 42)";
        try (final Statement statement = connection.createStatement()) {
            assertThat(statement.executeUpdate(sql), is(1));
        }
    }

    @Test
    @DisplayName("Test that prepared insert statement starting with comment is executed")
    public void testInsertPreparedStatement() throws SQLException {
        String sql = "-- some insert query\n INSERT INTO blackhole (entity, time, value) VALUES ('hole', 0, 42)";
        try (final PreparedStatement statement = connection.prepareStatement(sql)) {
            assertThat(statement.executeUpdate(), is(1));
        }
    }

    @Test
    @DisplayName("Test that update statement starting with comment is executed")
    public void testUpdateStatement() throws SQLException {
        String sql = "-- some update query\n UPDATE blackhole SET entity = 'hole', value = -1 WHERE time = 10000";
        try (final Statement statement = connection.createStatement()) {
            assertThat(statement.executeUpdate(sql), is(1));
        }
    }

    @Test
    @DisplayName("Test that prepared update statement starting with comment is executed")
    public void testUpdatePreparedStatement() throws SQLException {
        String sql = "-- some update query\n UPDATE blackhole SET entity = 'hole', value = -1 WHERE time = 10000";
        try (final PreparedStatement statement = connection.prepareStatement(sql)) {
            assertThat(statement.executeUpdate(), is(1));
        }
    }

    @Test
    @DisplayName("Test that batch prepared statement starting with comment is executed")
    public void testBatchPreparedStatement() throws SQLException {
        String sql = "-- some insert query\n INSERT INTO blackhole (entity, time, value) VALUES ('hole', ?, ?)";
        try (final PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 1; i < 5; i++) {
                statement.setLong(1, i * 100000L);
                statement.setDouble(2, Math.log(i));
                statement.addBatch();
            }
            assertThat(statement.executeBatch(), is(new int[] {1,1,1,1}));
        }
    }

    @Test
    @DisplayName("Test that batch statement starting with comment is executed")
    public void testBatchStatement() throws SQLException {
        String sql = "-- some insert query\n INSERT INTO blackhole (entity, time, value) VALUES ('hole', %s, %s)";
        try (final Statement statement = connection.createStatement()) {
            for (int i = 1; i < 5; i++) {
                statement.addBatch(String.format(sql, i * 10000L, Math.asin(i)));
            }
            assertThat(statement.executeBatch(), is(new int[] {1,1,1,1}));
        }
    }

    @Test
    @DisplayName("Test that statement query consisting of only comments is sent to ATSD")
    public void testStatementWithOnlyComments() throws SQLException {
        String sql = "-- i don't know how to write this query\n";
        try (final Statement statement = connection.createStatement()) {
            expectedException.expect(SQLException.class);
            expectedException.expectMessage(containsString("Syntax error at line 1 position 39: mismatched input '<EOF>' expecting SELECT"));
            statement.execute(sql);
        }
    }

    @Test
    @DisplayName("Test that prepared statement query consisting of only comments is sent to ATSD")
    public void testPreparedStatementWithOnlyComments() throws SQLException {
        String sql = "-- i don't know how to write this query\n";
        try (final PreparedStatement statement = connection.prepareStatement(sql)) {
            expectedException.expect(SQLException.class);
            expectedException.expectMessage(containsString("Syntax error at line 1 position 39: mismatched input '<EOF>' expecting SELECT"));
            statement.execute();
        }
    }
}
