package com.axibase.tsd.driver.jdbc.data;

import com.axibase.tsd.driver.jdbc.rules.OutputLogsToAllure;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.axibase.tsd.driver.jdbc.util.AtsdColumn.*;
import static util.TestProperties.*;
import static util.TestUtil.*;

public class InsertTest extends AbstractDataTest {
    private static final String INSERT = "INSERT INTO \"{}\" ({}, entity, value, tags) VALUES (?,?,?,?)";
    private static final double DEFAULT_VALUE = 123.456;

    private long currentTime;

    @Rule
    public OutputLogsToAllure outputLogsToAllure = new OutputLogsToAllure(REDIRECT_OUTPUT_TO_ALLURE);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setCurrentTime() {
        currentTime = System.currentTimeMillis();
    }

    @Test
    public void testStatement() throws SQLException, InterruptedException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        String pattern = "INSERT INTO \"{}\" (time, entity, value, tags) VALUES ({},'{}',{},{})";
        String sql = format(pattern, metricName, currentTime, entityName + "-1", DEFAULT_VALUE, null);
        try (Statement stmt = connection.createStatement()) {
            int res = stmt.executeUpdate(sql);
            Assert.assertEquals(1, res);
        }

        sql = "SELECT time, value FROM \"" + metricName + "\" WHERE entity='" + entityName + "-1' ORDER BY time DESC LIMIT 1";
        Map<String, Object> last = getLastInserted(connection, sql);
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, (long) last.get(TIME));
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TAGS));
    }

    @Test
    public void testStatementWithAtsdSeries() throws SQLException, InterruptedException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        String pattern = "INSERT INTO atsd_series (time, entity, metric, value, tags) VALUES ({},'{}','{}',{},{})";
        String sql = format(pattern, currentTime, entityName + "-2", metricName, DEFAULT_VALUE, null);
        try (Statement stmt = connection.createStatement()) {
            int res = stmt.executeUpdate(sql);
            Assert.assertEquals(1, res);
        }

        sql = "SELECT time, value FROM \"" + metricName + "\" WHERE entity='" + entityName + "-2' ORDER BY time DESC LIMIT 1";
        Map<String, Object> last = getLastInserted(connection, sql);
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, (long) last.get(TIME));
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TAGS));
    }

    @Test
    public void testStatementBatch() throws SQLException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        final String pattern1 = "INSERT INTO \"{}\" (time, entity, value, entity.tags, metric.tags) VALUES ({},'{}',{},{},{})";
        final String pattern2 = "INSERT INTO atsd_series (metric, time, entity, value, entity.tags, metric.tags) VALUES ('{}',{},'{}',{},{},{})";
        try (Statement stmt = connection.createStatement()) {
            stmt.addBatch(format(pattern1, metricName, currentTime + 1, entityName, DEFAULT_VALUE + 1, null, "'test1=value1'"));
            stmt.addBatch(format(pattern1, metricName, currentTime + 2, entityName, DEFAULT_VALUE + 2, "'test1=value1'", null));
            stmt.addBatch(format(pattern2, metricName, currentTime + 3, entityName, DEFAULT_VALUE + 3, null, null));
            stmt.addBatch(format(pattern2, metricName, currentTime + 4, entityName, DEFAULT_VALUE + 4, "'test1=value1'", "'test1=value1'"));
            int[] res = stmt.executeBatch();
            Assert.assertArrayEquals(new int[] {2,2,1,3}, res);
        }
    }

    @Test
    public void testStatementWithEntityColumns() throws SQLException, InterruptedException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        final String datetime = Instant.ofEpochMilli(currentTime).toString();
        final String entityLabel = entityName + "-label";
        final String entityTimeZone = "UTC";
        final String entityInterpolation = "linear";
        final String entityTagValue = "value1";
        final String pattern = "INSERT INTO \"{}\" (datetime, entity, value, tags, entity.label, entity.interpolate, entity.timeZone, entity.tags.test1)" +
                " VALUES ('{}','{}',{},{},'{}','{}','{}','{}')";
        String sql = format(pattern, metricName, datetime, entityName, DEFAULT_VALUE, null, entityLabel, entityInterpolation, entityTimeZone, entityTagValue);
        try (Statement stmt = connection.createStatement()) {
            int res = stmt.executeUpdate(sql);
            Assert.assertEquals(2, res);
        }
        sql = "SELECT time, value, text, tags, entity.label, entity.interpolate, entity.timeZone, entity.tags FROM \"" + metricName
                + "\" WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
        Map<String, Object> last = getLastInserted(connection, sql);
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, last.get(TIME));
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TEXT));
        Assert.assertNull(last.get(TAGS));
        Assert.assertEquals(entityLabel, last.get(ENTITY_LABEL));
        Assert.assertEquals(entityInterpolation.toUpperCase(), last.get(ENTITY_INTERPOLATE));
        Assert.assertEquals(entityTimeZone, last.get(ENTITY_TIME_ZONE));
        Assert.assertEquals("test1=" + entityTagValue, last.get(ENTITY_TAGS));
    }

    @Test
    public void testStatementWithMetricColumns() throws SQLException, InterruptedException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        final String datetime = Instant.ofEpochMilli(currentTime).toString();
        final String metricLabel = metricName + "-label";
        final String metricTagValue = "M1";
        final boolean metricEnabled = true;
        final String metricTimeZone = "UTC";
        final String metricInterpolation = "linear";
        final String metricDescription = "description 1";
        final boolean metricVersioning = false;
        final String metricFilter = "filter1";
        final String metricUnits = "units1";
        final String pattern = "INSERT INTO \"{}\" (datetime,entity,value,tags,metric.tags.test1,metric.label,metric.enabled" +
                ",metric.interpolate,metric.timeZone,metric.description,metric.versioning,metric.filter,metric.units) " +
                "VALUES ('{}','{}',{},{},'{}','{}',{},'{}','{}','{}',{},'{}','{}')";
        String sql = format(pattern, metricName, datetime, entityName, DEFAULT_VALUE, null, metricTagValue, metricLabel, metricEnabled, metricInterpolation,
                metricTimeZone, metricDescription, metricVersioning, metricFilter, metricUnits);
        try (Statement stmt = connection.createStatement()) {
            int res = stmt.executeUpdate(sql);
            Assert.assertEquals(2, res);
        }
        sql = "SELECT time, value, text, tags, metric.name, metric.tags, metric.label, metric.enabled, metric.interpolate, metric.timeZone" +
                ", metric.description, metric.versioning, metric.units, metric.minValue, metric.maxValue, metric.dataType, metric.filter" +
                ", metric.invalidValueAction, metric.lastInsertTime, metric.persistent, metric.retentionIntervalDays, metric.timePrecision" +
                " FROM \"" + metricName + "\" WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
        Map<String, Object> last = getLastInserted(connection, sql);
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, last.get(TIME));
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TEXT));
        Assert.assertNull(last.get(TAGS));
        Assert.assertEquals(metricName, last.get(METRIC_NAME));
        Assert.assertEquals("test1=" + metricTagValue, last.get(METRIC_TAGS));
        Assert.assertEquals(metricLabel, last.get(METRIC_LABEL));
        Assert.assertTrue(Boolean.valueOf((String) last.get(METRIC_ENABLED)));
        Assert.assertEquals(metricInterpolation.toUpperCase(), last.get(METRIC_INTERPOLATE));
        Assert.assertEquals(metricTimeZone, last.get(METRIC_TIME_ZONE));
        Assert.assertEquals(metricDescription, last.get(METRIC_DESCRIPTION));
        Assert.assertFalse(Boolean.valueOf((String) last.get(METRIC_VERSIONING)));
        Assert.assertEquals(metricUnits, last.get(METRIC_UNITS));
        Assert.assertNull(metricUnits, last.get(METRIC_MIN_VALUE));
        Assert.assertNull(metricUnits, last.get(METRIC_MAX_VALUE));
        Assert.assertEquals(metricFilter, last.get(METRIC_FILTER));
        Assert.assertEquals("NONE", last.get(METRIC_INVALID_VALUE_ACTION));
        Assert.assertNotNull(last.get(METRIC_LAST_INSERT_TIME));
        Assert.assertTrue(Boolean.valueOf((String) last.get(METRIC_PERSISTENT)));
        Assert.assertEquals("MILLISECONDS", last.get(METRIC_TIME_PRECISION));
    }

    @Test
    public void testPreparedStatmentBatch() throws SQLException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, TIME))) {
            for (int i = 0; i < 3; i++) {
                stmt.setLong(1, currentTime + i);
                stmt.setString(2, entityName + '-' + i);
                stmt.setDouble(3, i);
                stmt.setString(4, null);
                stmt.addBatch();
            }
            int[] res = stmt.executeBatch();
            Assert.assertArrayEquals(new int[]{1, 1, 1}, res);
        }
    }

    @Test
    public void testPreparedStatmentBatchWithAtsdSeries() throws SQLException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        String sql = "INSERT INTO atsd_series (time, entity, metric, value, tags) VALUES (?,?,?,?,?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (int i = 0; i < 3; i++) {
                stmt.setLong(1, currentTime + i);
                stmt.setString(2, entityName + '-' + i);
                stmt.setString(3, metricName);
                stmt.setDouble(4, i);
                stmt.setString(5, null);
                stmt.addBatch();
            }
            int[] res = stmt.executeBatch();
            Assert.assertArrayEquals(new int[]{1, 1, 1}, res);
        }
    }

    @Test
    public void testPreparedStatmentBatchWithMetaColumns() throws SQLException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        String sql = "INSERT INTO '" + metricName + "' (time, entity, value, entity.tags, metric.tags) VALUES (?,?,?,?,?)";
        List<List<Object>> batchValues = new ArrayList<>();
        batchValues.add(Arrays.asList(currentTime + 1, entityName, DEFAULT_VALUE + 1, null, "test1=value1"));
        batchValues.add(Arrays.asList(currentTime + 2, entityName, DEFAULT_VALUE + 2, "test1=value1", null));
        batchValues.add(Arrays.asList(currentTime + 3, entityName, DEFAULT_VALUE + 3, null, null));
        batchValues.add(Arrays.asList(currentTime + 4, entityName, DEFAULT_VALUE + 4, "test1=value1", "test1=value1"));
        try(PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (List<Object> values : batchValues) {
                stmt.setLong(1, (Long) values.get(0));
                stmt.setString(2, (String) values.get(1));
                stmt.setDouble(3, (Double) values.get(2));
                stmt.setString(4, (String) values.get(3));
                stmt.setString(5, (String) values.get(4));
                stmt.addBatch();
            }
            int[] res = stmt.executeBatch();
            Assert.assertArrayEquals(new int[] {2,2,1,3}, res);
        }
    }

    @Test
    public void testPreparedStatementWithDatetimeAsNumber() throws SQLException, InterruptedException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        insertDatetimeAsNumber(entityName, metricName + "-1", currentTime);
        insertDatetimeAsNumber(entityName, metricName + "-2", BigDecimal.valueOf(currentTime));
        insertDatetimeAsNumber(entityName, metricName + "-3", (double) currentTime);
    }

    private void insertDatetimeAsNumber(String entityName, String metricName, Number datetime) throws SQLException, InterruptedException {
        try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, DATETIME))) {
            if (datetime instanceof Long) {
                stmt.setLong(1, (long) datetime);
            } else if (datetime instanceof BigDecimal) {
                stmt.setBigDecimal(1, (BigDecimal) datetime);
            } else {
                stmt.setDouble(1, datetime.doubleValue());
            }
            stmt.setString(2, entityName);
            stmt.setDouble(3, DEFAULT_VALUE);
            stmt.setString(4, null);
            Assert.assertEquals(1, stmt.executeUpdate());
        }
        String sql = "SELECT datetime, value, tags FROM \"" + metricName + "\" WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
        Map<String, Object> last = getLastInserted(connection, sql);
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, ((Timestamp) last.get(DATETIME)).getTime());
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TAGS));
    }

    @Test
    public void testPreparedStatementWithDatetimeAsString() throws SQLException, InterruptedException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        final String datetime = Instant.ofEpochMilli(currentTime).toString();
        try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, DATETIME))) {
            stmt.setString(1, datetime);
            stmt.setString(2, entityName);
            stmt.setDouble(3, DEFAULT_VALUE);
            stmt.setString(4, null);
            Assert.assertEquals(1, stmt.executeUpdate());
        }
        String sql = "SELECT datetime, value, tags FROM \"" + metricName + "\" WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
        Map<String, Object> last = getLastInserted(connection, sql);
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, ((Timestamp) last.get(DATETIME)).getTime());
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TAGS));
    }

    @Test
    public void testPreparedStatementWithInvalidDatetime() throws SQLException {
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Invalid datetime value: test_datetime. Expected formats: yyyy-MM-dd'T'HH:mm:ss[.SSS]'Z', yyyy-MM-dd HH:mm:ss[.fffffffff]");
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, DATETIME))) {
            stmt.setString(1, "test_datetime");
            stmt.setString(2, entityName);
            stmt.setDouble(3, DEFAULT_VALUE);
            stmt.setString(4, null);
            Assert.assertEquals(1, stmt.executeUpdate());
        }
    }

    @Test
    public void testPreparedStatementWithInvalidTime() throws SQLException {
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Invalid value: test_time. Current type: String, expected type: Number");
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, TIME))) {
            stmt.setString(1, "test_time");
            stmt.setString(2, entityName);
            stmt.setDouble(3, DEFAULT_VALUE);
            stmt.setString(4, null);
            Assert.assertEquals(1, stmt.executeUpdate());
        }
    }

    @Test
    public void testPreparedStatementWithInvalidValue() throws SQLException {
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Invalid value: test_value. Current type: String, expected type: Number");
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, TIME))) {
            stmt.setLong(1, currentTime);
            stmt.setString(2, entityName);
            stmt.setString(3, "test_value");
            stmt.setString(4, null);
            Assert.assertEquals(1, stmt.executeUpdate());
        }
    }

    @Test
    public void testPreparedStatementWithNull() throws SQLException, InterruptedException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, TIME))) {
            stmt.setLong(1, currentTime);
            stmt.setString(2, entityName);
            stmt.setDouble(3, DEFAULT_VALUE);
            stmt.setString(4, null);
            Assert.assertEquals(1, stmt.executeUpdate());
        }
        final String sql = "SELECT time, value, text, tags FROM \"" + metricName + "\" WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
        Map<String, Object> last = getLastInserted(connection, sql);
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, last.get(TIME));
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TEXT));
        Assert.assertNull(last.get(TAGS));
    }

    @Test
    public void testPreparedStatementWithTags() throws SQLException, InterruptedException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        insertTags(entityName + "-1", metricName + "-1","t1=1");
        insertTags(entityName + "-2", metricName + "-2","t2=2;t3=3");
    }

    private void insertTags(final String entityName, final String metricName, final String tags) throws SQLException,
            InterruptedException {
        try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, TIME))) {
            stmt.setLong(1, currentTime);
            stmt.setString(2, entityName);
            stmt.setDouble(3, DEFAULT_VALUE);
            stmt.setString(4, tags);
            Assert.assertEquals(1, stmt.executeUpdate());
        }

        final String sql = "SELECT time, value, text, tags FROM \"" + metricName + "\" WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
        Map<String, Object> last = getLastInserted(connection, sql);
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, last.get(TIME));
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TEXT));
        Assert.assertEquals(tags, last.get(TAGS));
    }

    @Test
    public void testPreparedStatementWithEntityColumns() throws SQLException, InterruptedException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        String sql = "INSERT INTO '" + metricName + "-1' (datetime, entity, value, tags, entity.label, entity.tags) VALUES (?,?,?,?,?,?)";
        final String entityLabel = entityName + "-label";
        final String entityTags = "test1=value1";
        final String datetime = Instant.ofEpochMilli(currentTime).toString();
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, datetime);
            stmt.setString(2, entityName);
            stmt.setDouble(3, DEFAULT_VALUE);
            stmt.setString(4, null);
            stmt.setString(5, entityLabel);
            stmt.setString(6, entityTags);
            Assert.assertEquals(2, stmt.executeUpdate());
        }
        sql = "SELECT time, value, text, tags, entity.label, entity.tags FROM \"" + metricName
                + "-1\" WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
        Map<String, Object> last = getLastInserted(connection, sql);
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, last.get(TIME));
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TEXT));
        Assert.assertNull(last.get(TAGS));
        Assert.assertEquals(entityLabel, last.get(ENTITY_LABEL));
        Assert.assertEquals(entityTags, last.get(ENTITY_TAGS));

        sql = "INSERT INTO '" + metricName + "-2' (datetime, entity, value, tags, entity.label, entity.interpolate, entity.timeZone, entity.tags.test1)" +
                " VALUES (?,?,?,?,?,?,?,?)";
        final String entityTimeZone = "UTC";
        final String entityInterpolation = "linear";
        final String entityTagValue = "value1";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, datetime);
            stmt.setString(2, entityName);
            stmt.setDouble(3, DEFAULT_VALUE);
            stmt.setString(4, null);
            stmt.setString(5, entityLabel);
            stmt.setString(6, entityInterpolation);
            stmt.setString(7, entityTimeZone);
            stmt.setString(8, entityTagValue);
            Assert.assertEquals(2, stmt.executeUpdate());
        }
        sql = "SELECT time, value, text, tags, entity.label, entity.interpolate, entity.timeZone, entity.tags FROM \"" + metricName
                + "-2\" WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
        last = getLastInserted(connection, sql);
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, last.get(TIME));
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TEXT));
        Assert.assertNull(last.get(TAGS));
        Assert.assertEquals(entityLabel, last.get(ENTITY_LABEL));
        Assert.assertEquals(entityInterpolation.toUpperCase(), last.get(ENTITY_INTERPOLATE));
        Assert.assertEquals(entityTimeZone, last.get(ENTITY_TIME_ZONE));
        Assert.assertEquals("test1=" + entityTagValue, last.get(ENTITY_TAGS));
    }

    @Test
    public void testPreparedStatementWithEntityGroups() throws SQLException {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage(ENTITY_GROUPS);
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        String sql = "INSERT INTO '" + metricName + "' (time, entity, value, entity.groups) VALUES (?,?,?,?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, currentTime);
            stmt.setString(2, entityName);
            stmt.setDouble(3, DEFAULT_VALUE);
            stmt.setString(4, "group1");
            stmt.executeUpdate();
        }
    }

    @Test
    public void testPreparedStatementWithMetricLastInsertTime() throws SQLException {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage(METRIC_LAST_INSERT_TIME);
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        String sql = "INSERT INTO '" + metricName + "' (time, entity, value, metric.lastInsertTime) VALUES (?,?,?,?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, currentTime);
            stmt.setString(2, entityName);
            stmt.setDouble(3, DEFAULT_VALUE);
            stmt.setLong(4, System.currentTimeMillis());
            stmt.executeUpdate();
        }
    }

    @Test
    public void testPreparedStatementWithMetricPersistent() throws SQLException {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage(METRIC_PERSISTENT);
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        String sql = "INSERT INTO '" + metricName + "' (time, entity, value, metric.persistent) VALUES (?,?,?,?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, currentTime);
            stmt.setString(2, entityName);
            stmt.setDouble(3, DEFAULT_VALUE);
            stmt.setBoolean(4, false);
            stmt.executeUpdate();
        }
    }

    @Test
    public void testPreparedStatementWithMetricRetentionIntervalDays() throws SQLException {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage(METRIC_RETENTION_INTERVAL_DAYS);
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        String sql = "INSERT INTO '" + metricName + "' (time, entity, value, metric.retentionIntervalDays) VALUES (?,?,?,?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, currentTime);
            stmt.setString(2, entityName);
            stmt.setDouble(3, DEFAULT_VALUE);
            stmt.setInt(4, 1);
            stmt.executeUpdate();
        }
    }

    @Test
    public void testPreparedStatementWithMetricTimePrecision() throws SQLException {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage(METRIC_TIME_PRECISION);
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        String sql = "INSERT INTO '" + metricName + "' (time, entity, value, metric.timePrecision) VALUES (?,?,?,?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, currentTime);
            stmt.setString(2, entityName);
            stmt.setDouble(3, DEFAULT_VALUE);
            stmt.setString(4, "seconds");
            stmt.executeUpdate();
        }
    }
}
