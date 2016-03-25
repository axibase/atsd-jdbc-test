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
package com.axibase.tsd.driver.jdbc;

import java.sql.DriverManager;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.strategies.StrategyFactory;

public class TestProperties implements TestConstants {
	private static final LoggingFacade logger = LoggingFacade.getLogger(TestProperties.class);
	protected static int RETRIES = 1;
	protected static Boolean TRUST_URL;
	protected static String HTTP_ATDS_URL;
	protected static String JDBC_ATDS_URL;
	protected static String LOGIN_NAME;
	protected static String LOGIN_PASSWORD;
	protected static Integer TINY_TABLE_COUNT;
	protected static String TINY_TABLE;
	protected static String SMALL_TABLE;
	protected static String MEDIUM_TABLE;
	protected static String LARGE_TABLE;
	protected static String HUGE_TABLE;
	protected static String JUMBO_TABLE;
	protected static String TWO_TABLES;
	protected static String WRONG_TABLE;
	protected static String READ_STRATEGY;
	protected static AtsdDriver driver;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String trustProp = System.getProperty("axibase.tsd.driver.jdbc.trust");
		TRUST_URL = trustProp != null ? Boolean.valueOf(trustProp) : null;
		LOGIN_NAME = System.getProperty("axibase.tsd.driver.jdbc.username");
		LOGIN_PASSWORD = System.getProperty("axibase.tsd.driver.jdbc.password");
		HTTP_ATDS_URL = System.getProperty("axibase.tsd.driver.jdbc.url");
		final StringBuilder sb = new StringBuilder(JDBC_ATDS_URL_PREFIX).append(HTTP_ATDS_URL);
		if (TRUST_URL != null)
			sb.append(TRUST_URL.booleanValue() ? TRUST_PARAMETER_IN_QUERY : UNTRUST_PARAMETER_IN_QUERY);
		READ_STRATEGY = System.getProperty("axibase.tsd.driver.jdbc.strategy");
		if (READ_STRATEGY != null) {
			if (TRUST_URL == null)
				sb.append(PARAM_SEPARATOR);
			sb.append(READ_STRATEGY.equalsIgnoreCase(StrategyFactory.FILE_STRATEGY) ? STRATEGY_FILE_PARAMETER
					: STRATEGY_STREAM_PARAMETER);
		}
		JDBC_ATDS_URL = sb.toString();
		String counted = System.getProperty("axibase.tsd.driver.jdbc.metric.tiny.count");
		TINY_TABLE_COUNT = !StringUtils.isEmpty(counted) ? Integer.parseInt(counted) : -1;
		TINY_TABLE = System.getProperty("axibase.tsd.driver.jdbc.metric.tiny");
		SMALL_TABLE = System.getProperty("axibase.tsd.driver.jdbc.metric.small");
		MEDIUM_TABLE = System.getProperty("axibase.tsd.driver.jdbc.metric.medium");
		LARGE_TABLE = System.getProperty("axibase.tsd.driver.jdbc.metric.large");
		HUGE_TABLE = System.getProperty("axibase.tsd.driver.jdbc.metric.huge");
		JUMBO_TABLE = System.getProperty("axibase.tsd.driver.jdbc.metric.jumbo");
		TWO_TABLES = System.getProperty("axibase.tsd.driver.jdbc.metric.concurrent");
		WRONG_TABLE = System.getProperty("axibase.tsd.driver.jdbc.metric.wrong");
		driver = new AtsdDriver();
		if (logger.isDebugEnabled())
			logger.debug("System properies has been set");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		DriverManager.deregisterDriver(driver);
	}

}