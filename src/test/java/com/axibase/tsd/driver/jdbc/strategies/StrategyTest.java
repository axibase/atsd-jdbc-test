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
package com.axibase.tsd.driver.jdbc.strategies;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.axibase.tsd.driver.jdbc.DriverConstants;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.axibase.tsd.driver.jdbc.TestProperties;
import com.axibase.tsd.driver.jdbc.content.ContentDescription;
import com.axibase.tsd.driver.jdbc.content.StatementContext;
import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.axibase.tsd.driver.jdbc.intf.IContentProtocol;
import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import com.axibase.tsd.driver.jdbc.protocol.ProtocolFactory;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;
import com.axibase.tsd.driver.jdbc.strategies.storage.FileStoreStrategy;
import com.axibase.tsd.driver.jdbc.strategies.stream.KeepAliveStrategy;

public class StrategyTest extends TestProperties {
	private static final Logger logger = LoggerFactory.getLogger(StrategyTest.class);

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testFullPassOnTiny() throws Exception {
		if (StringUtils.isEmpty(TINY_TABLE))
			return;
		String[] last = fullPassOnTable(TINY_TABLE);
		assertNotNull(last);
	}

	@Test
	public final void testFullPassOnSmall() throws Exception {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		String[] last = fullPassOnTable(SMALL_TABLE);
		assertNotNull(last);
	}

	@Test
	public final void testFullPassOnMedium() throws Exception {
		if (StringUtils.isEmpty(MEDIUM_TABLE))
			return;
		String[] last = fullPassOnTable(MEDIUM_TABLE);
		assertNotNull(last);
	}

	@Test
	public final void testFullPassOnLarge() throws Exception {
		if (StringUtils.isEmpty(LARGE_TABLE))
			return;
		String[] last = fullPassOnTable(LARGE_TABLE);
		assertNotNull(last);
	}

	private String[] fullPassOnTable(String table) throws Exception {
		final List<String> params = new ArrayList<String>();
		if (TRUST_URL != null) {
			params.add(PARAM_SEPARATOR + DriverConstants.TRUST_PARAM_NAME + '=' + TRUST_URL);
		}
		boolean isDefault = READ_STRATEGY == null || READ_STRATEGY.equalsIgnoreCase("stream");
		params.add(isDefault ? STRATEGY_STREAM_PARAMETER : STRATEGY_FILE_PARAMETER);
		final ContentDescription cd = new ContentDescription(HTTP_ATDS_URL, SELECT_ALL_CLAUSE + table, LOGIN_NAME,
				LOGIN_PASSWORD, params.toArray(new String[params.size()]));
		final IContentProtocol tp = ProtocolFactory.create(SdkProtocolImpl.class, cd);
		tp.readContent();
		StatementContext context = new StatementContext();
		try (final IStoreStrategy strategy = isDefault ? new KeepAliveStrategy(context)
				: new FileStoreStrategy(context); final InputStream is = tp.readContent();) {
			assertNotNull(is);
			strategy.store(is);
			final String[] header = strategy.openToRead();
			assertNotNull(header);
			if (logger.isDebugEnabled())
				logger.debug("Header: " + Arrays.toString(header));
			return fetchAllRecords(strategy);
		}
	}

	private String[] fetchAllRecords(final IStoreStrategy strategy) throws IOException, AtsdException {
		int pos = 0;
		String[] last = null;
		while (true) {
			final List<String[]> fetched = strategy.fetch(pos, 100);
			assertNotNull(fetched);
			int size = fetched.size();
			if (size != 100) {
				if (size != 0) {
					last = fetched.get(size - 1);
				}
				if (logger.isDebugEnabled())
					logger.debug(Arrays.toString(last));
				return last;
			} else {
				last = fetched.get(99);
			}
			pos += size;
			if (pos % 100000 == 0 && logger.isDebugEnabled()) {
				logger.debug(String.format("In progress - %s", pos));
			}
		}
	}

}
