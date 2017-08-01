package com.axibase.tsd.driver.jdbc;

import lombok.experimental.UtilityClass;

@UtilityClass
public final class TestConstants {
	public static final String SELECT_TVE_CLAUSE = "SELECT time, value, entity FROM ";
	public static final String SELECT_DVE_CLAUSE = "SELECT datetime, value, entity FROM ";
	public static final String SELECT_ALL_CLAUSE = "SELECT * FROM ";
	public static final String SELECT_LIMIT_1000 = " LIMIT 1000";
	public static final String SELECT_LIMIT_100000 = " LIMIT 100000";
    public static final String WHERE_CLAUSE = " WHERE entity = ?";
}