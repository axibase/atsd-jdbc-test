package com.axibase.tsd.driver.jdbc.ext;

import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;

import java.sql.*;

import static org.junit.Assert.assertNotNull;

public class LogUtil {
	public static int printResultSet(final ResultSet resultSet, LoggingFacade logger) throws AtsdException, SQLException {
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

	public static void logTime(long start, String name, LoggingFacade logger) {
		if (logger.isDebugEnabled())
			logger.debug(String.format("Test [%s] is done in %d msecs", name, (System.currentTimeMillis() - start)));
	}
}
