package com.axibase.tsd.driver.jdbc.data;

import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Stopwatch;
import org.junit.runner.Description;
import org.junit.runner.RunWith;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static com.axibase.tsd.driver.jdbc.TestConstants.SELECT_ALL_CLAUSE;
import static com.axibase.tsd.driver.jdbc.TestConstants.WHERE_CLAUSE;
import static com.axibase.tsd.driver.jdbc.data.DataFromProvidedTablesTest.checkRemoteStatement;

@Slf4j
@RunWith(DataProviderRunner.class)
public class DataFromDefaultTablesTest extends AbstractDataTest {

    @Rule
    public Stopwatch stopwatch = new Stopwatch() {
        @Override
        protected void succeeded(long nanos, Description description) {
            log.info("Test {} finished in {} ms ", description, TimeUnit.NANOSECONDS.toMillis(nanos));
        }

        @Override
        protected void failed(long nanos, Throwable e, Description description) {
            log.info("Test {} finished in {} ms ", description, TimeUnit.NANOSECONDS.toMillis(nanos));
        }
    };

    @DataProvider
    public static Object[][] queries() {
        return new Object[][] {
                { "SELECT * FROM cpu_busy OUTER JOIN disk_used WHERE time > now - 1 * hour" },
                { "SELECT * FROM cpu_busy JOIN cpu_idle WHERE time > now - 1 * hour" },
                { "SELECT entity, time, AVG(cpu_busy.value), AVG(disk_used.value) FROM cpu_busy OUTER JOIN disk_used WHERE time > now - 1 * hour GROUP BY entity, period(15 minute)" },
                { "SELECT entity, time, AVG(cpu_busy.value) FROM cpu_busy WHERE time > now - 1 * hour GROUP BY entity, period(15 minute) WITH row_number(entity, tags ORDER BY time DESC) <= 3" },
                { "SELECT entity, datetime, AVG(cpu_busy.value) FROM cpu_busy WHERE time > now - 1 * hour GROUP BY entity, period(15 minute) WITH time > last_time - 30 * minute" },
                { "SELECT entity, disk_used.time, cpu_busy.time, AVG(cpu_busy.value), AVG(disk_used.value), tags.* FROM cpu_busy JOIN USING entity disk_used WHERE time > now - 1 * hour GROUP BY entity, tags, period(15 minute)" }
        };
    }

    @Test
    @UseDataProvider("queries")
    public void testRemoteStatementWithJoins(String query) throws AtsdException, SQLException {
        final long count = checkRemoteStatement(query, NO_RETRIES);
        log.info("Returned {} rows for query '{}'", count, query);
    }

    @Test
    public final void testStatementsWithCondition() throws AtsdException, SQLException {
        checkRemoteStatement(
                "SELECT entity, datetime, value, tags.mount_point, tags.file_system FROM df.disk_used_percent WHERE entity = 'NURSWGHBS001' AND datetime > now - 1 * HOUR LIMIT 10", NO_RETRIES);
    }

    @Test
    public final void testPreparedStatementsWithArgs() throws AtsdException, SQLException {
        checkRemotePreparedStatementWithLimits(
                "SELECT time, value, tags.file_system FROM df.disk_used_percent WHERE tags.file_system LIKE ? AND datetime between ? and ?",
                new String[]{"tmpfs", "2015-07-08T16:00:00Z", "2017-07-08T16:30:00Z"}, 1001, 10001);
    }

    @Test
    public final void testPreparedStatementsWithAggregation() throws AtsdException, SQLException {
        checkRemotePreparedStatementWithLimits(
                "SELECT count(*), entity, tags.*, period (30 minute) FROM df.disk_used "
                        + "WHERE entity = ? AND tags.mount_point = ? AND tags.file_system = ? "
                        + "AND datetime BETWEEN ? AND ? GROUP BY entity, tags, period (30 minute)",
                new String[]{"nurswgvml502", "/run", "tmpfs", "2015-07-08T16:00:00Z", "2017-07-08T16:30:00Z"}, 1001,
                10001);
    }

    @Test
    public final void testRemotePreparedStatementsWithArg() throws AtsdException, SQLException {
        checkRemotePreparedStatementWithLimits(SELECT_ALL_CLAUSE + "cpu_busy" + WHERE_CLAUSE,
                new String[]{"nurswgvml212"}, 1001, 10001);

    }
}
