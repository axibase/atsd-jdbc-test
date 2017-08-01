package com.axibase.tsd.driver.jdbc.data;

import com.axibase.tsd.driver.jdbc.DriverTestBase;
import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import util.TableConstants;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.axibase.tsd.driver.jdbc.TestConstants.SELECT_ALL_CLAUSE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static util.TestProperties.*;

@Slf4j
public abstract class AbstractDataTest extends DriverTestBase {
    protected static final int NO_RETRIES = 1;

    protected static Connection connection;

    @BeforeClass
    public static void setUp() throws SQLException {
        connection = DriverManager.getConnection(DEFAULT_JDBC_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD);
    }

    @AfterClass
    public static void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    protected void checkRemoteStatementWithDifferentResultSets() throws AtsdException, SQLException {
        try (final Statement statement = connection.createStatement()) {
            String[] metrics = TableConstants.TWO_TABLES.split(",");
            for (String metric : metrics) {
                int count = executeAndReturnNumberOfLines(statement, SELECT_ALL_CLAUSE + metric);
                assertThat(count, is(not(0)));
            }
        }
    }


    protected void checkStatementWithTraversingSimultaneously() throws AtsdException, SQLException, InterruptedException {
        ExecutorService service = Executors.newFixedThreadPool(2);
        try (final Statement statement = connection.createStatement()) {
            String[] metrics = TableConstants.TWO_TABLES.split(",");
            for (final String metric : metrics) {
                service.submit(() -> {
                    try {
                        int count = executeAndReturnNumberOfLines(statement, SELECT_ALL_CLAUSE + metric);
                        assertThat(count , is(not(0)));
                    } catch (SQLException | AtsdException e) {
                        log.error(e.getMessage(), e);
                        Assert.fail();
                    }
                });
            }
            boolean result = service.awaitTermination(5, TimeUnit.SECONDS);
            log.debug("Service is terminated: {}", result);
        } finally {
            service.shutdown();
        }
    }

    static int checkRemoteStatementWithLimits(String sql, int fetchSize, int maxRows)
            throws AtsdException, SQLException {
        try (final Statement statement = connection.createStatement()) {
            statement.setFetchSize(fetchSize);
            statement.setMaxRows(maxRows);
            return executeAndReturnNumberOfLines(statement, sql);
        }
    }

    int checkRemotePreparedStatementNoArgs(String sql) throws AtsdException, SQLException {
        try (final PreparedStatement prepareStatement = connection.prepareStatement(sql)) {
            return executeAndReturnNumberOfLines(prepareStatement);
        }
    }

    static int checkRemotePreparedStatementWithLimits(String sql, String[] args, int fetchSize, int maxRows)
            throws AtsdException, SQLException {
        try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setFetchSize(fetchSize);
            preparedStatement.setMaxRows(maxRows);
            int num = 1;
            for (String arg : args) {
                preparedStatement.setString(num++, arg);
            }
            return executeAndReturnNumberOfLines(preparedStatement);
        }
    }

    static int executeAndReturnNumberOfLines(Statement statement, String sql) throws SQLException, AtsdException {
        try (final ResultSet resultSet = statement.executeQuery(sql)) {
            return executeAndReturnNumberOfLinesHelper(resultSet, sqlWarning -> {});
        }
    }

    static int executeAndReturnNumberOfLines(PreparedStatement statement) throws SQLException, AtsdException {
        try (final ResultSet resultSet = statement.executeQuery()) {
            return executeAndReturnNumberOfLinesHelper(resultSet, sqlWarning -> {});
        }
    }

    static int executeAndReturnNumberOfLinesHelper(ResultSet resultSet, Consumer<SQLWarning> warningConsumer) throws AtsdException, SQLException {
        int count = 0;
        assertThat(resultSet, is(notNullValue()));
        final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        assertThat(resultSetMetaData, is(notNullValue()));
        log.debug("Columns:");
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            log.debug("{}\t{}    \t{}",
                    resultSetMetaData.getColumnType(i),
                    resultSetMetaData.getColumnName(i),
                    resultSetMetaData.getColumnTypeName(i));
        }
        log.trace("Data:");
        final StringBuilder buffer = new StringBuilder();
        while (resultSet.next()) {
            buffer.setLength(0);
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                int type = resultSetMetaData.getColumnType(i);
                if (i > 1) {
                    buffer.append("     \t");
                }
                buffer.append(type).append(':');
                switch (type) {
                    case Types.VARCHAR:
                        buffer.append("getString: ").append(resultSet.getString(i));
                        break;
                    case Types.INTEGER:
                        buffer.append("getInt: ").append(resultSet.getInt(i));
                        break;
                    case Types.BIGINT:
                        buffer.append("getLong: ").append(resultSet.getLong(i));
                        break;
                    case Types.SMALLINT:
                        buffer.append("getShort: ").append(resultSet.getShort(i));
                        break;
                    case Types.REAL:
                        buffer.append("getFloat: ").append(resultSet.getFloat(i));
                        break;
                    case Types.DOUBLE:
                        buffer.append("getDouble: ").append(resultSet.getDouble(i));
                        break;
                    case Types.DECIMAL:
                        buffer.append("getDecimal: ").append(resultSet.getBigDecimal(i));
                        break;
                    case Types.TIMESTAMP:
                        buffer.append("getTimestamp: ").append(resultSet.getTimestamp(i).toString());
                        break;
                    default:
                        throw new UnsupportedOperationException("Unknown JDBC type " + type);
                }
            }
            count++;

            log.trace(buffer.toString());

        }

        log.debug("Total: {}", count);

        warningConsumer.accept(resultSet.getWarnings());

        return count;
    }
}
