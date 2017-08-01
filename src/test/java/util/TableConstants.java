package util;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
@Slf4j
public final class TableConstants {
    public static final String TINY_TABLE_COUNT_KEY = "axibase.tsd.driver.jdbc.metric.tiny.count";
    public static final String TINY_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.tiny";
    public static final String SMALL_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.small";
    public static final String MEDIUM_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.medium";
    public static final String LARGE_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.large";
    public static final String HUGE_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.huge";
    public static final String JUMBO_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.jumbo";
    public static final String TWO_TABLES_KEY = "axibase.tsd.driver.jdbc.metric.concurrent";
    public static final String WRONG_TABLE_KEY = "axibase.tsd.driver.jdbc.metric.wrong";

    public static final long TINY_TABLE_COUNT = getCount(System.getProperty(TINY_TABLE_COUNT_KEY));
    public static final String TINY_TABLE = System.getProperty(TINY_TABLE_KEY);
    public static final String SMALL_TABLE = System.getProperty(SMALL_TABLE_KEY);
    public static final String MEDIUM_TABLE = System.getProperty(MEDIUM_TABLE_KEY);
    public static final String LARGE_TABLE = System.getProperty(LARGE_TABLE_KEY);
    public static final String HUGE_TABLE = System.getProperty(HUGE_TABLE_KEY);
    public static final String JUMBO_TABLE = System.getProperty(JUMBO_TABLE_KEY);
    public static final String TWO_TABLES = System.getProperty(TWO_TABLES_KEY);
    public static final String WRONG_TABLE = System.getProperty(WRONG_TABLE_KEY);

    static {
        log.info("tiny table: " + TINY_TABLE + ", count = " + TINY_TABLE_COUNT);
        log.info("small table: " + SMALL_TABLE);
        log.info("medium table: " + MEDIUM_TABLE);
        log.info("large table: " + LARGE_TABLE);
        log.info("huge table: " + HUGE_TABLE);
    }

    private static long getCount(String countVar) {
        return !StringUtils.isEmpty(countVar) ? Long.parseLong(countVar) : -1L;
    }
}
