package com.axibase.tsd.driver.jdbc.ext;


import com.axibase.tsd.driver.jdbc.AtsdDriver;
import com.axibase.tsd.driver.jdbc.TestProperties;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Enumeration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

@RunWith(DataProviderRunner.class)
public class LimitTest extends TestProperties {
	private static final LoggingFacade logger = LoggingFacade.getLogger(LimitTest.class);
	protected AtsdDriver driver;

	@DataProvider
	public static Object[][] dataQueryLimitMaxRowsResult() {
		return new Object[][] {
				{ 5, 3, 3 },
				{ 5, 10, 5 },
				{ 5, null, 5 },
				{ 0, 3, 3 },
				{ null, 3, 3 }
		};
	}

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

	@Test
	@UseDataProvider("dataQueryLimitMaxRowsResult")
	public void testStatementLimit(Integer maxRows, Integer queryLimit, int expectedResultsetSize) throws SQLException, AtsdException {
		StringBuilder sql = new StringBuilder(SELECT_ALL_CLAUSE).append(TINY_TABLE);
		if (queryLimit != null) {
			sql.append(" LIMIT ").append(queryLimit);
		}
		try (final Connection connection = DriverManager.getConnection(JDBC_ATDS_URL, LOGIN_NAME, LOGIN_PASSWORD);
			 final Statement statement = connection.createStatement()) {
			if (maxRows != null) {
				statement.setMaxRows(maxRows);
			}
			final ResultSet resultSet = statement.executeQuery(sql.toString());
			assertThat(getResultSetSize(resultSet), is(expectedResultsetSize));
		}
	}

	@Test
	public void testStatementWithoutLimits() throws SQLException, AtsdException {
		final String sql = SELECT_ALL_CLAUSE + TINY_TABLE;
		try (final Connection connection = DriverManager.getConnection(JDBC_ATDS_URL, LOGIN_NAME, LOGIN_PASSWORD);
			 final Statement statement = connection.createStatement();
			 final ResultSet resultSet = statement.executeQuery(sql)) {

			if (TINY_TABLE_COUNT != -1) {
				assertThat(getResultSetSize(resultSet), is(TINY_TABLE_COUNT));
			}
		}
	}

	@Test
	@UseDataProvider("dataQueryLimitMaxRowsResult")
	public void testPreparedStatementLimit(Integer maxRows, Integer queryLimit, int expectedResultsetSize) throws SQLException, AtsdException {
		StringBuilder sql = new StringBuilder(SELECT_ALL_CLAUSE).append(TINY_TABLE);
		if (queryLimit != null) {
			sql.append(" LIMIT ").append(queryLimit);
		}
		try (final Connection connection = DriverManager.getConnection(JDBC_ATDS_URL, LOGIN_NAME, LOGIN_PASSWORD);
			 final PreparedStatement statement = connection.prepareStatement(sql.toString())) {
			if (maxRows != null) {
				statement.setMaxRows(maxRows);
			}
			final ResultSet resultSet = statement.executeQuery();
			assertThat(getResultSetSize(resultSet), is(expectedResultsetSize));
		}
	}

	@Test
	public void testPreparedStatementWithoutLimits() throws SQLException, AtsdException {
		final String sql = SELECT_ALL_CLAUSE + TINY_TABLE;
		try (final Connection connection = DriverManager.getConnection(JDBC_ATDS_URL, LOGIN_NAME, LOGIN_PASSWORD);
			 final PreparedStatement statement = connection.prepareStatement(sql);
			 final ResultSet resultSet = statement.executeQuery()) {

			if (TINY_TABLE_COUNT != -1) {
				assertThat(getResultSetSize(resultSet), is(TINY_TABLE_COUNT));
			}
		}
	}

	private static int getResultSetSize(ResultSet resultSet) throws SQLException {
		int resultSetSize = 0;
		while(resultSet.next()) {
			++resultSetSize;
		}
		return resultSetSize;
	}

}
