package com.axibase.tsd.driver.jdbc.data;

import io.qameta.allure.junit4.DisplayName;
import org.junit.Test;
import util.TestUtil;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ResultSetMetadataDatatypeTest extends AbstractDataTest {
    @Test
    @DisplayName("Test that metadata reports trade data time as decimal value")
    public void testQueryTrades() throws SQLException {
        String query = "SELECT time, * from atsd_trade WHERE class = 'IEXG' AND symbol = 'TSLA' LIMIT 1";
        try (final Statement statement = connection.createStatement();
             final ResultSet rs = statement.executeQuery(query)) {
            final ResultSetMetaData metaData = rs.getMetaData();
            final List<String> labels = TestUtil.resolveColumnLabels(metaData);
            final int timeIndex = labels.indexOf("time") + 1;
            assertThat(metaData.getColumnTypeName(timeIndex)).isEqualTo("decimal");
            assertThat(rs.next()).describedAs("Result set should not be empty").isTrue();
            assertThat(rs.getObject(timeIndex)).isInstanceOf(BigDecimal.class);
        }
    }

    @Test
    @DisplayName("Test that metadata reports series time as bigint")
    public void testQuerySeries() throws SQLException {
        String query = "SELECT time, * from jvm_memory_used LIMIT 1";
        try (final Statement statement = connection.createStatement();
             final ResultSet rs = statement.executeQuery(query)) {
            final ResultSetMetaData metaData = rs.getMetaData();
            final List<String> labels = TestUtil.resolveColumnLabels(metaData);
            final int timeIndex = labels.indexOf("time") + 1;
            assertThat(metaData.getColumnTypeName(timeIndex)).isEqualTo("bigint");
            assertThat(rs.next()).describedAs("Result set should not be empty").isTrue();
            assertThat(rs.getObject(timeIndex)).isInstanceOf(Long.class);
        }
    }
}
