/*
* Copyright 2016 Axibase Corporation or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* https://www.axibase.com/atsd/axibase-apache-2.0.pdf
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package com.axibase.tsd.driver.jdbc.ext;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.Enumeration;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.axibase.tsd.driver.jdbc.AtsdDriver;
import com.axibase.tsd.driver.jdbc.TestProperties;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;

public class RemoteConnectionTest extends TestProperties {
	private static final LoggingFacade logger = LoggingFacade.getLogger(RemoteConnectionTest.class);
	protected AtsdDriver driver;

	@Before
	public void setUp() throws Exception {
		DriverManager.registerDriver(new AtsdDriver());
		final Enumeration<Driver> drivers = DriverManager.getDrivers();
		while (drivers.hasMoreElements()) {
			final Driver nextElement = drivers.nextElement();
			if (logger.isDebugEnabled())
				logger.debug("Driver: " + nextElement);
		}
		try {
			driver = (AtsdDriver) DriverManager.getDriver(JDBC_ATDS_URL);
		} catch (SQLException e) {
			fail(e.getMessage());
		}
	}

	@After
	public void tearDown() throws Exception {
		DriverManager.deregisterDriver(driver);
	}

	//@Test
	public final void tinyRemoteStatement() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(TINY_TABLE))
			return;
		int count = checkRemoteStatement(SELECT_ALL_CLAUSE + TINY_TABLE);
		assertTrue(TINY_TABLE_COUNT == -1 || count == TINY_TABLE_COUNT);
		if (logger.isDebugEnabled())
			logger.debug(String.format("[%s]%d", new Object() {
			}.getClass().getEnclosingMethod().getName(), count));
	}

	//@Test
	public final void smallRemoteStatement() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		int count = checkRemoteStatement(SELECT_ALL_CLAUSE + SMALL_TABLE);
		if (logger.isDebugEnabled())
			logger.debug(String.format("[%s]%d", new Object() {
			}.getClass().getEnclosingMethod().getName(), count));
	}

	//@Test
	public final void mediumRemoteStatement() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(MEDIUM_TABLE))
			return;
		int count = checkRemoteStatement(SELECT_ALL_CLAUSE + MEDIUM_TABLE);
		if (logger.isDebugEnabled())
			logger.debug(String.format("[%s]%d", new Object() {
			}.getClass().getEnclosingMethod().getName(), count));
	}

	//@Test
	public final void largeRemoteStatement() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(LARGE_TABLE))
			return;
		int count = checkRemoteStatement(SELECT_ALL_CLAUSE + LARGE_TABLE);
		if (logger.isDebugEnabled())
			logger.debug(String.format("[%s]%d", new Object() {
			}.getClass().getEnclosingMethod().getName(), count));
	}

	//@Test
	public final void hugeRemoteStatement() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(HUGE_TABLE))
			return;
		int count = checkRemoteStatement(SELECT_ALL_CLAUSE + HUGE_TABLE);
		if (logger.isDebugEnabled())
			logger.debug(String.format("[%s]%d", new Object() {
			}.getClass().getEnclosingMethod().getName(), count));
	}

	//@Test
	public final void testRemoteStatementWithFields() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(TINY_TABLE))
			return;
		int count = checkRemoteStatement(SELECT_TVE_CLAUSE + TINY_TABLE);
		assertTrue(TINY_TABLE_COUNT == -1 || count == TINY_TABLE_COUNT);
	}

	//@Test
	public final void testRemoteStatementWithDates() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(TINY_TABLE))
			return;
		int count = checkRemoteStatement(SELECT_DVE_CLAUSE + TINY_TABLE);
		assertTrue(TINY_TABLE_COUNT == -1 || count == TINY_TABLE_COUNT);
	}

	//@Test
	public final void testRemoteStatementWithJoins() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(TINY_TABLE) || !TINY_TABLE.toLowerCase(Locale.US).endsWith("cpu_busy"))
			return;
		int count;
		count = checkRemoteStatement("SELECT * FROM cpu_busy OUTER JOIN disk_used WHERE time > now - 1 * hour");
		if (logger.isDebugEnabled())
			logger.debug(String.format("[%s-1]%d", new Object() {
			}.getClass().getEnclosingMethod().getName(), count));
		count = checkRemoteStatement("SELECT * FROM cpu_busy JOIN cpu_idle WHERE time > now - 1 * hour");
		if (logger.isDebugEnabled())
			logger.debug(String.format("[%s-2]%d", new Object() {
			}.getClass().getEnclosingMethod().getName(), count));
		count = checkRemoteStatement(
				"SELECT entity, time, AVG(cpu_busy.value), AVG(disk_used.value) FROM cpu_busy OUTER JOIN disk_used WHERE time > now - 1 * hour GROUP BY entity, period(15 minute)");
		if (logger.isDebugEnabled())
			logger.debug(String.format("[%s-3]%d", new Object() {
			}.getClass().getEnclosingMethod().getName(), count));
		count = checkRemoteStatement(
				"SELECT entity, time, AVG(cpu_busy.value) FROM cpu_busy WHERE time > now - 1 * hour GROUP BY entity, period(15 minute) WITH row_number(entity, tags ORDER BY time DESC) <= 3");
		if (logger.isDebugEnabled())
			logger.debug(String.format("[%s-4]%d", new Object() {
			}.getClass().getEnclosingMethod().getName(), count));
		count = checkRemoteStatement(
				"SELECT entity, datetime, AVG(cpu_busy.value) FROM cpu_busy WHERE time > now - 1 * hour GROUP BY entity, period(15 minute) WITH time > last_time - 30 * minute");
		if (logger.isDebugEnabled())
			logger.debug(String.format("[%s-5]%d", new Object() {
			}.getClass().getEnclosingMethod().getName(), count));
		count = checkRemoteStatement(
				"SELECT entity, disk_used.time, cpu_busy.time, AVG(cpu_busy.value), AVG(disk_used.value), tags.* FROM cpu_busy JOIN USING entity disk_used WHERE time > now - 1 * hour GROUP BY entity, tags, period(15 minute)");
		if (logger.isDebugEnabled())
			logger.debug(String.format("[%s-6]%d", new Object() {
			}.getClass().getEnclosingMethod().getName(), count));
	}

	//@Test
	public final void testRemotePreparedStatement() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(TINY_TABLE))
			return;
		int count = checkRemotePreparedStatementNoArgs(SELECT_DVE_CLAUSE + TINY_TABLE);
		assertTrue(TINY_TABLE_COUNT == -1 || count == TINY_TABLE_COUNT);
	}

	//@Test
	public final void testRemotePreparedStatementsWithArg() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(TINY_TABLE) || !TINY_TABLE.toLowerCase(Locale.US).endsWith("cpu_busy"))
			return;
		checkRemotePreparedStatementWithLimits(SELECT_ALL_CLAUSE + TINY_TABLE + WHERE_CLAUSE,
				new String[] { "nurswgvml212" }, 1001, 10001);
	}

	//@Test
	public final void smallRemoteStatementTwice() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		RETRIES = 2;
		int count = checkRemoteStatement(SELECT_ALL_CLAUSE + SMALL_TABLE);
		if (logger.isDebugEnabled())
			logger.debug(String.format("[%s]%d", new Object() {
			}.getClass().getEnclosingMethod().getName(), count));
		RETRIES = 1;
	}

	//@Test
	public final void testStatementsWithCondition() throws AtsdException, SQLException {
		checkRemoteStatement(
				"SELECT entity, datetime, value, tags.mount_point, tags.file_system FROM df.disk_used_percent WHERE entity = 'NURSWGHBS001' AND datetime > now - 1 * HOUR LIMIT 10");
	}

	//@Test
	public final void testPreparedStatementsWithArgs() throws AtsdException, SQLException {
		checkRemotePreparedStatementWithLimits(
				"SELECT time, value, tags.file_system FROM df.disk_used_percent WHERE tags.file_system LIKE ? AND datetime between ? and ?",
				new String[] { "tmpfs", "2015-07-08T16:00:00Z", "2017-07-08T16:30:00Z" }, 1001, 10001);
	}

	//@Test
	public final void testPreparedStatementsWithAggregation() throws AtsdException, SQLException {
		checkRemotePreparedStatementWithLimits(
				"SELECT count(*), entity, tags.*, period (30 minute) FROM df.disk_used "
						+ "WHERE entity = ? AND tags.mount_point = ? AND tags.file_system = ? "
						+ "AND datetime BETWEEN ? AND ? GROUP BY entity, tags, period (30 minute)",
				new String[] { "nurswgvml502", "/run", "tmpfs", "2015-07-08T16:00:00Z", "2017-07-08T16:30:00Z" }, 1001,
				10001);
	}

	//@Test
	public final void testRemoteStatementsOnSmall() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		checkRemoteStatementWithLimits(SELECT_ALL_CLAUSE + SMALL_TABLE + SELECT_LIMIT_1000, 1001, 10001);
	}

	//@Test
	public final void testRemoteStatementsOnMedium() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(MEDIUM_TABLE))
			return;
		checkRemoteStatementWithLimits(SELECT_ALL_CLAUSE + MEDIUM_TABLE + SELECT_LIMIT_100000, 10001, 100001);
	}

	//@Test
	public final void testRemoteStatementsOnLarge() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(LARGE_TABLE))
			return;
		checkRemoteStatementWithLimits(SELECT_ALL_CLAUSE + LARGE_TABLE + SELECT_LIMIT_100000, 100001, 1000001);
	}

	//@Test
	public final void remoteStatementWithDifferentResultSets() throws AtsdException, SQLException {
		checkRemoteStatementWithDifferentResultSets();
	}

	//@Test
	public final void remoteStatementWithTraversingSimultaneously()
			throws AtsdException, SQLException, InterruptedException {
		checkStatementWithTraversingSimultaneously();
	}

	//@Test
	public final void testRemoteStatementsWithLimits() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(TINY_TABLE))
			return;
		int count = checkRemoteStatementWithLimits(SELECT_ALL_CLAUSE + TINY_TABLE, 101, 10001);
		assertTrue(TINY_TABLE_COUNT == -1 || count == TINY_TABLE_COUNT);
	}

	//@Test(expected = UnsupportedOperationException.class)
	public final void smallRemoteStatementWithAbsPos() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(TINY_TABLE))
			throw new UnsupportedOperationException();
		checkRemoteStatementWithAbsolute(SELECT_ALL_CLAUSE + TINY_TABLE);
	}

	//@Test(expected = UnsupportedOperationException.class)
	public final void smallRemoteStatementWithRelPos() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(TINY_TABLE))
			throw new UnsupportedOperationException();
		checkRemoteStatementWithRelative(SELECT_ALL_CLAUSE + TINY_TABLE);
	}

	//@Test(expected = SQLException.class)
	public final void wrongRemoteStatement() throws AssertionError, AtsdException, SQLException {
		if (StringUtils.isEmpty(WRONG_TABLE))
			throw new SQLException();
		checkRemoteStatement(SELECT_ALL_CLAUSE + WRONG_TABLE);
	}

	private int checkRemoteStatement(String sql) throws AtsdException, SQLException {
		long start = System.currentTimeMillis();
		try (final Connection connection = DriverManager.getConnection(JDBC_ATDS_URL, LOGIN_NAME, LOGIN_PASSWORD);
				final Statement statement = connection.createStatement();) {
			int count = 0;
			for (int i = 0; i < RETRIES; i++) {
				try (final ResultSet resultSet = statement.executeQuery(sql);) {
					count = printResultSet(resultSet);
				}
			}
			return count;
		} finally {
			logTime(start, new Object() {
			}.getClass().getEnclosingMethod().getName());
		}
	}

	private void checkRemoteStatementWithDifferentResultSets() throws AtsdException, SQLException {
		if (StringUtils.isEmpty(TWO_TABLES))
			return;
		long start = System.currentTimeMillis();
		try (final Connection connection = DriverManager.getConnection(JDBC_ATDS_URL, LOGIN_NAME, LOGIN_PASSWORD);
				final Statement statement = connection.createStatement();) {
			String[] metrics = TWO_TABLES.split(",");
			for (String metric : metrics) {
				try (final ResultSet resultSet = statement.executeQuery(SELECT_ALL_CLAUSE + metric);) {
					int count = printResultSet(resultSet);
					assertTrue(count != 0);
				}
			}
		} finally {
			logTime(start, new Object() {
			}.getClass().getEnclosingMethod().getName());
		}
	}

	private void checkStatementWithTraversingSimultaneously() throws AtsdException, SQLException, InterruptedException {
		if (StringUtils.isEmpty(TWO_TABLES))
			return;
		long start = System.currentTimeMillis();
		ExecutorService service = Executors.newFixedThreadPool(2);
		try (final Connection connection = DriverManager.getConnection(JDBC_ATDS_URL, LOGIN_NAME, LOGIN_PASSWORD);
				final Statement statement = connection.createStatement();) {
			String[] metrics = TWO_TABLES.split(",");
			for (final String metric : metrics) {
				service.submit(new Runnable() {
					public void run() {
						try (final ResultSet resultSet = statement.executeQuery(SELECT_ALL_CLAUSE + metric);) {
							int count = printResultSet(resultSet);
							assertTrue(count != 0);
						} catch (SQLException | AtsdException e) {
							logger.error(e.getMessage(), e);
							fail();
						}
					}
				});
			}
			boolean result = service.awaitTermination(5, TimeUnit.SECONDS);
			if (logger.isDebugEnabled())
				logger.debug("Service is terminated: {}", result);
		} finally {
			service.shutdown();
			logTime(start, new Object() {
			}.getClass().getEnclosingMethod().getName());
		}
	}

	private int checkRemoteStatementWithAbsolute(String sql) throws AtsdException, SQLException {
		long start = System.currentTimeMillis();
		try (final Connection connection = DriverManager.getConnection(JDBC_ATDS_URL, LOGIN_NAME, LOGIN_PASSWORD);
				final Statement statement = connection.createStatement();
				final ResultSet resultSet = statement.executeQuery(sql);) {
			resultSet.absolute(100);
			return printResultSet(resultSet);
		} finally {
			logTime(start, new Object() {
			}.getClass().getEnclosingMethod().getName());
		}
	}

	private int checkRemoteStatementWithRelative(String sql) throws AtsdException, SQLException {
		long start = System.currentTimeMillis();
		try (final Connection connection = DriverManager.getConnection(JDBC_ATDS_URL, LOGIN_NAME, LOGIN_PASSWORD);
				final Statement statement = connection.createStatement();
				final ResultSet resultSet = statement.executeQuery(sql);) {
			resultSet.relative(100);
			return printResultSet(resultSet);
		} finally {
			logTime(start, new Object() {
			}.getClass().getEnclosingMethod().getName());
		}
	}

	private int checkRemoteStatementWithLimits(String sql, int fetchSize, int maxRows)
			throws AtsdException, SQLException {
		long start = System.currentTimeMillis();
		try (final Connection connection = DriverManager.getConnection(JDBC_ATDS_URL, LOGIN_NAME, LOGIN_PASSWORD);
				final Statement statement = connection.createStatement();) {
			statement.setFetchSize(fetchSize);
			statement.setMaxRows(maxRows);
			try (final ResultSet resultSet = statement.executeQuery(sql);) {
				return printResultSet(resultSet);
			}
		} finally {
			logTime(start, new Object() {
			}.getClass().getEnclosingMethod().getName());
		}
	}

	private int checkRemotePreparedStatementNoArgs(String sql) throws AtsdException, SQLException {
		long start = System.currentTimeMillis();
		try (final Connection connection = DriverManager.getConnection(JDBC_ATDS_URL, LOGIN_NAME, LOGIN_PASSWORD);
				final PreparedStatement prepareStatement = connection.prepareStatement(sql);
				final ResultSet resultSet = prepareStatement.executeQuery();) {
			return printResultSet(resultSet);
		} finally {
			logTime(start, new Object() {
			}.getClass().getEnclosingMethod().getName());
		}
	}

	private int checkRemotePreparedStatementWithLimits(String sql, String[] args, int fetchSize, int maxRows)
			throws AtsdException, SQLException {
		long start = System.currentTimeMillis();
		try (final Connection connection = DriverManager.getConnection(JDBC_ATDS_URL, LOGIN_NAME, LOGIN_PASSWORD);
				final PreparedStatement prepareStatement = connection.prepareStatement(sql);) {
			prepareStatement.setFetchSize(fetchSize);
			prepareStatement.setMaxRows(maxRows);
			int num = 1;
			for (String arg : args) {
				prepareStatement.setString(num++, arg);
			}
			try (final ResultSet resultSet = prepareStatement.executeQuery();) {
				return printResultSet(resultSet);
			}
		} finally {
			logTime(start, new Object() {
			}.getClass().getEnclosingMethod().getName());
		}
	}

	private int printResultSet(final ResultSet resultSet) throws AtsdException, SQLException {
		assertNotNull(resultSet);
		final ResultSetMetaData rsmd = resultSet.getMetaData();
		assertNotNull(rsmd);
		if (logger.isDebugEnabled())
			logger.debug("Columns:");
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			int type = rsmd.getColumnType(i);
			String name = rsmd.getColumnName(i);
			String typeName = rsmd.getColumnTypeName(i);
			if (logger.isDebugEnabled())
				logger.debug(String.format("%s\t%s    \t%s", type, name, typeName));
		}
		if (logger.isTraceEnabled())
			logger.trace("Data:");
		int count = 0;
		StringBuilder sb;
		while (resultSet.next()) {
			sb = new StringBuilder();
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				int type = rsmd.getColumnType(i);
				if (i > 1)
					sb.append("     \t");
				sb.append(type).append(':');
				switch (type) {
				case Types.VARCHAR:
					sb.append("getString: ").append(resultSet.getString(i));
					break;
				case Types.INTEGER:
					sb.append("getInt: ").append(resultSet.getInt(i));
					break;
				case Types.BIGINT:
					sb.append("getLong: ").append(resultSet.getLong(i));
					break;
				case Types.SMALLINT:
					sb.append("getShort: ").append(resultSet.getShort(i));
					break;
				case Types.FLOAT:
					sb.append("getFloat: ").append(resultSet.getFloat(i));
					break;
				case Types.DOUBLE:
					sb.append("getDouble: ").append(resultSet.getDouble(i));
					break;
				case Types.DECIMAL:
					sb.append("getDecimal: ").append(resultSet.getBigDecimal(i));
					break;
				case Types.TIMESTAMP:
					sb.append("getTimestamp: ").append(resultSet.getTimestamp(i).toString());
					break;
				default:
					throw new UnsupportedOperationException();
				}
			}
			count++;
			if (logger.isTraceEnabled()) {
				logger.trace(sb.toString());
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Total: " + count);
		}
		final SQLWarning warnings = resultSet.getWarnings();
		if (warnings != null)
			logger.error(warnings.getMessage(), warnings);
		return count;
	}

	private void logTime(long start, String name) {
		if (logger.isDebugEnabled())
			logger.debug(String.format("Test [%s] is done in %d msecs", name, (System.currentTimeMillis() - start)));
	}
}
