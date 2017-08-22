package com.axibase.tsd.driver.jdbc.data;

import com.axibase.tsd.driver.jdbc.rules.OutputLogsToAllure;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.axibase.tsd.driver.jdbc.util.AtsdColumn.*;
import static util.TestProperties.REDIRECT_OUTPUT_TO_ALLURE;
import static util.TestUtil.*;


public class UpdateTest extends AbstractDataTest {
    private static final double DEFAULT_VALUE = 123.456;

    private long currentTime;

    @Rule
    public OutputLogsToAllure outputLogsToAllure = new OutputLogsToAllure(REDIRECT_OUTPUT_TO_ALLURE);


    @Before
    public void initCurrentTime()  {
        currentTime = System.currentTimeMillis();
    }

    @Test
    public void testStatement() throws SQLException, InterruptedException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        final String pattern = "UPDATE \"{}\" SET time={}, value={}, tags={} WHERE entity='{}'";
        String sql = format(pattern, metricName, currentTime, DEFAULT_VALUE, null, entityName);
        try (Statement stmt = connection.createStatement()) {
            int res = stmt.executeUpdate(sql);
            Assert.assertEquals(1, res);
        }

        sql = "SELECT time, value FROM \"" + metricName + "\" WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
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
        final String pattern = "UPDATE atsd_series SET time={}, value={}, tags={} WHERE entity='{}' and metric='{}'";
        String sql = format(pattern, currentTime, DEFAULT_VALUE, null, entityName, metricName);
        try (Statement stmt = connection.createStatement()) {
            int res = stmt.executeUpdate(sql);
            Assert.assertEquals(1, res);
        }

        sql = "SELECT time, value FROM \"" + metricName + "\" WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
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
        final String pattern1 = "UPDATE \"{}\" SET time={}, value={}, entity.tags={}, metric.tags={} WHERE entity='{}'";
        final String pattern2 = "UPDATE atsd_series SET time={}, value={}, entity.tags={}, metric.tags={} WHERE entity='{}' and metric='{}'";
        try (Statement stmt = connection.createStatement()) {
            stmt.addBatch(format(pattern1, metricName, currentTime + 1, DEFAULT_VALUE + 1, null, "'test1=value1'", entityName));
            stmt.addBatch(format(pattern1, metricName, currentTime + 2, DEFAULT_VALUE + 2, "'test1=value1'", null, entityName));
            stmt.addBatch(format(pattern2, currentTime + 3, DEFAULT_VALUE + 3, null, null, entityName, metricName));
            stmt.addBatch(format(pattern2, currentTime + 4, DEFAULT_VALUE + 4, "'test1=value1'", "'test1=value1'", entityName, metricName));
            int[] res = stmt.executeBatch();
            Assert.assertArrayEquals(new int[] {2,2,1,3}, res);
        }
    }

    @Test
    public void testPreparedStatmentBatch() throws SQLException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        String sql = "UPDATE \"" + metricName + "\" SET time=?, value=?, tags=? WHERE entity=?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (int i = 0; i < 3; i++) {
                stmt.setLong(1, currentTime + i);
                stmt.setDouble(2, i);
                stmt.setString(3, null);
                stmt.setString(4, entityName + '-' + i);
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

        String sql = "UPDATE atsd_series SET time=?, value=?, tags=? WHERE entity=? and metric=?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (int i = 0; i < 3; i++) {
                stmt.setLong(1, currentTime + i);
                stmt.setDouble(2, i);
                stmt.setString(3, null);
                stmt.setString(4, entityName + '-' + i);
                stmt.setString(5, metricName);
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
        String sql = "UPDATE \"" + metricName + "\" SET time=?, value=?, entity.tags=?, metric.tags=? WHERE entity=?";
        List<List<Object>> batchValues = new ArrayList<>();
        batchValues.add(Arrays.asList(currentTime + 1, DEFAULT_VALUE + 1, null, "test1=value1", entityName));
        batchValues.add(Arrays.asList(currentTime + 2, DEFAULT_VALUE + 2, "test1=value1", null, entityName));
        batchValues.add(Arrays.asList(currentTime + 3, DEFAULT_VALUE + 3, null, null, entityName));
        batchValues.add(Arrays.asList(currentTime + 4, DEFAULT_VALUE + 4, "test1=value1", "test1=value1", entityName));
        try(PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (List<Object> values : batchValues) {
                stmt.setLong(1, (Long) values.get(0));
                stmt.setDouble(2, (Double) values.get(1));
                stmt.setString(3, (String) values.get(2));
                stmt.setString(4, (String) values.get(3));
                stmt.setString(5, (String) values.get(4));
                stmt.addBatch();
            }
            int[] res = stmt.executeBatch();
            Assert.assertArrayEquals(new int[] {2,2,1,3}, res);
        }
    }

    @Test
    public void testPreparedStatementWithMetricColumns() throws SQLException, InterruptedException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        String sql = "UPDATE \"" + metricName + "-1\" SET datetime=?, value=?, tags=?, metric.label=?, metric.tags=? WHERE entity=?";
        final String metricLabel = metricName + "-label";
        final String metricTags = "test1=value1";
        final String datetime = Instant.ofEpochMilli(currentTime).toString();
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, datetime);
            stmt.setDouble(2, DEFAULT_VALUE);
            stmt.setString(3, null);
            stmt.setString(4, metricLabel);
            stmt.setString(5, metricTags);
            stmt.setString(6, entityName);
            Assert.assertEquals(2, stmt.executeUpdate());
        }
        sql = "SELECT time, value, text, tags, metric.label, metric.tags FROM \"" + metricName
                + "-1\" WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
        Map<String, Object> last = getLastInserted(connection, sql);
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, last.get(TIME));
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TEXT));
        Assert.assertNull(last.get(TAGS));
        Assert.assertEquals(metricLabel, last.get(METRIC_LABEL));
        Assert.assertEquals(metricTags, last.get(METRIC_TAGS));

        sql = "UPDATE \"" + metricName + "-2\" SET datetime=?, value=?, tags=?, metric.tags.test1=?, metric.label=?, metric.enabled=?, metric.interpolate=?" +
                ", metric.timeZone=?, metric.description=?, metric.versioning=?, metric.filter=?, metric.units=? WHERE entity=?";
        final String metricTagValue = "M1";
        final boolean metricEnabled = true;
        final String metricTimeZone = "UTC";
        final String metricInterpolation = "linear";
        final String metricDescription = "description 1";
        final boolean metricVersioning = false;
        final String metricFilter = "filter1";
        final String metricUnits = "units1";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, datetime);
            stmt.setDouble(2, DEFAULT_VALUE);
            stmt.setString(3, null);
            stmt.setString(4, metricTagValue);
            stmt.setString(5, metricLabel);
            stmt.setBoolean(6, metricEnabled);
            stmt.setString(7, metricInterpolation);
            stmt.setString(8, metricTimeZone);
            stmt.setString(9, metricDescription);
            stmt.setBoolean(10, metricVersioning);
            stmt.setString(11, metricFilter);
            stmt.setString(12, metricUnits);
            stmt.setString(13, entityName);
            Assert.assertEquals(2, stmt.executeUpdate());
        }
        sql = "SELECT time, value, text, tags, metric.name, metric.tags, metric.label, metric.enabled, metric.interpolate, metric.timeZone" +
                ", metric.description, metric.versioning, metric.units, metric.minValue, metric.maxValue, metric.dataType, metric.filter" +
                ", metric.invalidValueAction, metric.lastInsertTime, metric.persistent, metric.retentionIntervalDays, metric.timePrecision" +
                " FROM \"" + metricName + "-2\" WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
        last = getLastInserted(connection, sql);
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, last.get(TIME));
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TEXT));
        Assert.assertNull(last.get(TAGS));
        Assert.assertEquals(metricName + "-2", last.get(METRIC_NAME));
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
    public void testPreparedStatmentWithLikeOperator() throws SQLException, InterruptedException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        updateWithLikeOperator(entityName + "-1", metricName, false, false);
        updateWithLikeOperator(entityName + "-2", metricName, true, false);
        updateWithLikeOperator(entityName + "-3", metricName, false, true);
    }

    private void updateWithLikeOperator(String entityName, String metricName, boolean appendMetric, boolean withEscape) throws SQLException,
            InterruptedException {
        StringBuilder buffer = new StringBuilder();
        buffer.append("UPDATE atsd_series SET time=?, value=?, tags.tag1=? where entity='").append(entityName).append("' and metric like ");
        if (appendMetric) {
            buffer.append('\'').append(metricName).append('\'');
        } else {
            buffer.append('?');
        }
        if (withEscape) {
            buffer.append(" escape '#'");
        }
        try(PreparedStatement stmt = connection.prepareStatement(buffer.toString())) {
            stmt.setLong(1, currentTime);
            stmt.setDouble(2, DEFAULT_VALUE);
            stmt.setString(3, "value1");
            if (!appendMetric) {
                stmt.setString(4, withEscape ? metricName.replace("-", "#-") : metricName);
            }
            int res = stmt.executeUpdate();
            Assert.assertEquals(1, res);
        }

        String sql = "SELECT time, value, text, tags FROM atsd_series WHERE metric='{}' and entity='{}' ORDER BY time DESC LIMIT 1";
        Map<String, Object> last = getLastInserted(connection, format(sql, metricName, entityName));
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, last.get(TIME));
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TEXT));
        Assert.assertEquals("tag1=value1",last.get(TAGS));
    }

    @Test
    public void testPreparedStatmentWithIsNull() throws SQLException, InterruptedException {
        final String entityName = buildVariableName(ENTITY);
        final String metricName = buildVariableName(METRIC);
        String sql = "UPDATE \"" + metricName +  "\" SET time=?, value=? where entity='" + entityName + "' and tags.tag1 IS NULL";
        try(PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, currentTime);
            stmt.setDouble(2, DEFAULT_VALUE);
            int res = stmt.executeUpdate();
            Assert.assertEquals(1, res);
        }

        sql = "SELECT time, value, text, tags FROM \"" + metricName + "\" WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
        Map<String, Object> last = getLastInserted(connection, sql);
        Assert.assertFalse("No results", last.isEmpty());
        Assert.assertEquals(currentTime, last.get(TIME));
        Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
        Assert.assertNull(last.get(TEXT));
        Assert.assertNull(last.get(TAGS));
    }

}
