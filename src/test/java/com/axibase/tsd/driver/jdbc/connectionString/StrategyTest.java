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
package com.axibase.tsd.driver.jdbc.connectionString;

import com.axibase.tsd.driver.jdbc.DriverConstants;
import com.axibase.tsd.driver.jdbc.content.ContentDescription;
import com.axibase.tsd.driver.jdbc.content.StatementContext;
import com.axibase.tsd.driver.jdbc.enums.Location;
import com.axibase.tsd.driver.jdbc.enums.Strategy;
import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.axibase.tsd.driver.jdbc.intf.IContentProtocol;
import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import com.axibase.tsd.driver.jdbc.protocol.ProtocolFactory;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;
import com.axibase.tsd.driver.jdbc.rules.OutputLogsToAllure;
import com.axibase.tsd.driver.jdbc.strategies.StrategyFactory;
import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import com.axibase.tsd.driver.jdbc.rules.ExecuteWhenSysVariableSet;
import com.axibase.tsd.driver.jdbc.rules.SkipTestOnCondition;
import util.TestUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static com.axibase.tsd.driver.jdbc.TestConstants.SELECT_ALL_CLAUSE;
import static org.junit.Assert.assertNotNull;
import static util.TableConstants.*;
import static util.TestProperties.*;
import static util.TestUtil.prepareMetadata;

@Slf4j
@AllArgsConstructor
@RunWith(Parameterized.class)
public class StrategyTest {
	private final String strategy;

	@Parameterized.Parameters
	public static Collection<Object[]> strategies() {
		final List<Object[]> strategies = new ArrayList<>();
		for (Strategy strategy : Strategy.values()) {
			strategies.add(new Object[]{strategy.name().toLowerCase(Locale.US)});
		}
		return strategies;
	}

	@Rule
	public final SkipTestOnCondition skipTestOnCondition = new SkipTestOnCondition();

	@Rule
	public final OutputLogsToAllure outputLogsToAllure = new OutputLogsToAllure(REDIRECT_OUTPUT_TO_ALLURE);

	@Test
	@ExecuteWhenSysVariableSet(TINY_TABLE_KEY)
	public final void testFullPassOnTiny() throws Exception {
		List<Object> last = fullPassOnTable(TINY_TABLE);
		assertNotNull(last);
	}

	@Test
	@ExecuteWhenSysVariableSet(SMALL_TABLE_KEY)
	public final void testFullPassOnSmall() throws Exception {
		List<Object> last = fullPassOnTable(SMALL_TABLE);
		assertNotNull(last);
	}

	@Test
	@ExecuteWhenSysVariableSet(MEDIUM_TABLE_KEY)
	public final void testFullPassOnMedium() throws Exception {
		List<Object> last = fullPassOnTable(MEDIUM_TABLE);
		assertNotNull(last);
	}

	@Test
	@ExecuteWhenSysVariableSet(LARGE_TABLE_KEY)
	public final void testFullPassOnLarge() throws Exception {
		List<Object> last = fullPassOnTable(LARGE_TABLE);
		assertNotNull(last);
	}

	private List<Object> fullPassOnTable(String table) throws Exception {
		final AtsdConnectionInfo atsdConnectionInfo = new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)
				.withStrategy(strategy)
				.composeConnectionInfo();
		final ContentDescription contentDescription = new ContentDescription(
				Location.SQL_ENDPOINT.getUrl(atsdConnectionInfo), atsdConnectionInfo,
				SELECT_ALL_CLAUSE + table, TestUtil.createStatementContext());
		final IContentProtocol protocol = ProtocolFactory.create(SdkProtocolImpl.class, contentDescription);
		protocol.readContent(0);
		StatementContext context = TestUtil.createStatementContext();
		try (final IStoreStrategy strategy = StrategyFactory.create(
				StrategyFactory.findClassByName(READ_STRATEGY), context, DriverConstants.DEFAULT_ON_MISSING_METRIC_VALUE);
			 final InputStream inputStream = protocol.readContent(0)) {
			assertNotNull(inputStream);
			strategy.store(inputStream);
			final String[] header = strategy.openToRead(prepareMetadata("time,datetime,value,text,metric,entity,tags"));
			assertNotNull(header);

			log.debug("Header: {}", Arrays.toString(header));
			return fetchAllRecords(strategy);
		}
	}

	private List<Object> fetchAllRecords(final IStoreStrategy strategy) throws IOException, AtsdException {
		int pos = 0;
		List<Object> last = null;
		while (true) {
			final List<List<Object>> fetched = strategy.fetch(pos, 100);
			assertNotNull(fetched);
			int size = fetched.size();
			if (size != 100) {
				if (size != 0) {
					last = fetched.get(size - 1);
				}

				log.debug(StringUtils.join(last, ", "));

				return last;
			} else {
				last = fetched.get(99);
			}
			pos += size;
			if (pos % 100000 == 0) {
				log.debug("In progress - {}", pos);
			}
		}
	}
}
