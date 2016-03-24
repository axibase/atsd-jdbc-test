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
package com.axibase.tsd.driver.jdbc.content;

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.axibase.tsd.driver.jdbc.TestProperties;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;

public class DataProviderTest extends TestProperties {
	private static final LoggingFacade logger = LoggingFacade.getLogger(DataProvider.class);

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testSecureByDefault() throws Exception {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		try (DataProvider provider = new DataProvider(HTTP_ATDS_URL,
				SELECT_ALL_CLAUSE + SMALL_TABLE + SELECT_LIMIT_1000, LOGIN_NAME, LOGIN_PASSWORD,
				new StatementContext());) {
			provider.checkScheme(provider.getContentDescription().getQuery());
		}
	}

	@Test
	public final void testSecureTrusted() throws Exception {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		try (DataProvider provider = new DataProvider(HTTP_ATDS_URL + TRUST_PARAMETER_IN_QUERY,
				SELECT_ALL_CLAUSE + SMALL_TABLE + SELECT_LIMIT_1000, LOGIN_NAME, LOGIN_PASSWORD,
				new StatementContext());) {
			provider.checkScheme(provider.getContentDescription().getQuery());
		}
	}

	@Test
	public final void testSecureUntrusted() throws Exception {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		try (DataProvider provider = new DataProvider(HTTP_ATDS_URL + UNTRUST_PARAMETER_IN_QUERY,
				SELECT_ALL_CLAUSE + SMALL_TABLE + SELECT_LIMIT_1000, LOGIN_NAME, LOGIN_PASSWORD,
				new StatementContext());) {
			provider.checkScheme(provider.getContentDescription().getQuery());
		}
	}

	@Test
	public final void testCheckScheme() throws Exception {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		try (DataProvider provider = new DataProvider(HTTP_ATDS_URL,
				SELECT_ALL_CLAUSE + SMALL_TABLE + SELECT_LIMIT_1000, LOGIN_NAME, LOGIN_PASSWORD,
				new StatementContext());) {
			final ContentDescription contentDescription = provider.getContentDescription();
			provider.checkScheme(contentDescription.getQuery());
		}
	}

	@Test
	public final void testGetContentDescription() throws Exception {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		try (DataProvider provider = new DataProvider(HTTP_ATDS_URL + UNTRUST_PARAMETER_IN_QUERY,
				SELECT_ALL_CLAUSE + SMALL_TABLE + SELECT_LIMIT_1000, LOGIN_NAME, LOGIN_PASSWORD,
				new StatementContext());) {
			final ContentDescription contentDescription = provider.getContentDescription();
			assertNotNull(contentDescription);
			if (logger.isDebugEnabled()) {
				logger.debug(contentDescription.getHost());
				logger.debug(Arrays.toString(contentDescription.getParams()));
			}
		}
	}

	@Test
	public final void testClose() throws Exception {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		try (DataProvider provider = new DataProvider(HTTP_ATDS_URL,
				SELECT_ALL_CLAUSE + SMALL_TABLE + SELECT_LIMIT_1000, LOGIN_NAME, LOGIN_PASSWORD,
				new StatementContext());) {
			if (logger.isDebugEnabled()) {
				logger.debug(provider.toString());
			}
		}
	}

}
