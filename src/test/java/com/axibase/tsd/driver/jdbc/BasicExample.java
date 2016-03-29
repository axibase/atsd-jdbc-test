package com.axibase.tsd.driver.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BasicExample extends TestProperties {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	 @Test
	public void testData() throws ClassNotFoundException, SQLException {
		Class.forName("com.axibase.tsd.driver.jdbc.AtsdDriver");
		// String url = "jdbc:axibase:atsd:https://10.102.0.6:8443/api/sql";
		String query = "SELECT entity, datetime, value, tags.mount_point, tags.file_system "
				+ "FROM df.disk_used_percent WHERE entity = 'NURSWGHBS001' AND datetime > now - 1 * HOUR LIMIT 10";
		try (Connection connection = DriverManager.getConnection(JDBC_ATDS_URL, LOGIN_NAME, LOGIN_PASSWORD);
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(query);) {
			int rowNumber = 1;
			while (resultSet.next()) {
				System.out.print(rowNumber++);
				System.out.print("\tentity = " + resultSet.getString("entity"));
				System.out.print("\tdatetime = " + resultSet.getTimestamp("datetime").toString());
				System.out.print("\tvalue = " + resultSet.getString("value"));
				System.out.print("\ttags.mount_point = " + resultSet.getString("tags.mount_point"));
				System.out.println("\ttags.file_system = " + resultSet.getString("tags.file_system"));
			}
			final SQLWarning warnings = resultSet.getWarnings();
			if (warnings != null)
				warnings.printStackTrace();
		}

	}

	@Test
	public void testMetadata() throws ClassNotFoundException, SQLException {
		Class.forName("com.axibase.tsd.driver.jdbc.AtsdDriver");
		String url = "jdbc:axibase:atsd:https://10.102.0.6:8443/api/sql";
		try (Connection connection = DriverManager.getConnection(url, "axibase", "axibase");
				Statement statement = connection.createStatement();) {
			final DatabaseMetaData metaData = connection.getMetaData();
			final String databaseProductName = metaData.getDatabaseProductName();
			final String databaseProductVersion = metaData.getDatabaseProductVersion();
			final String driverName = metaData.getDriverName();
			final String driverVersion = metaData.getDriverVersion();
			System.out.println("Product Name:   \t" + databaseProductName);
			System.out.println("Product Version:\t" + databaseProductVersion);
			System.out.println("Driver Name:    \t" + driverName);
			System.out.println("Driver Version: \t" + driverVersion);
			System.out.println("\nTypeInfo:");
			ResultSet rs = metaData.getTypeInfo();
			while (rs.next()) {
				String name = rs.getString("TYPE_NAME");
				int type = rs.getInt("DATA_TYPE");
				int precision = rs.getInt("PRECISION");
				boolean isCS = rs.getBoolean("CASE_SENSITIVE");
				System.out.println(
						String.format("\tName:%s \tCS: %s \tType: %s \tPrecision: %s", name, isCS, type, precision));
			}
			System.out.println("\nTableTypes:");
			rs = metaData.getTableTypes();
			while (rs.next()) {
				final String type = rs.getString(1);
				System.out.println('\t' + type);
			}
			rs = metaData.getCatalogs();
			while (rs.next()) {
				String catalog = rs.getString(1);
				System.out.println("\nCatalog: \t" + catalog);
				ResultSet rs1 = metaData.getSchemas(catalog, null);
				while (rs1.next()) {
					final String schema = rs1.getString(1);
					System.out.println("Schema: \t" + schema);
				}
			}

		}
	}

}