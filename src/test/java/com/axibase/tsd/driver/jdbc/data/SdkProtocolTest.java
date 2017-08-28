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
package com.axibase.tsd.driver.jdbc.data;

import com.axibase.tsd.driver.jdbc.DriverTestBase;
import com.axibase.tsd.driver.jdbc.content.ContentDescription;
import com.axibase.tsd.driver.jdbc.enums.Location;
import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.axibase.tsd.driver.jdbc.intf.IContentProtocol;
import com.axibase.tsd.driver.jdbc.protocol.ProtocolFactory;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;
import com.axibase.tsd.driver.jdbc.rules.ExecuteWhenSysVariableSet;
import com.axibase.tsd.driver.jdbc.rules.SkipTestOnCondition;
import com.opencsv.CSVReader;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import util.TestUtil;

import java.io.*;
import java.security.GeneralSecurityException;
import java.util.Arrays;

import static com.axibase.tsd.driver.jdbc.TestConstants.SELECT_LIMIT_1000;
import static com.axibase.tsd.driver.jdbc.TestConstants.SELECT_TVE_CLAUSE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertNotNull;
import static util.TableConstants.SMALL_TABLE;
import static util.TableConstants.SMALL_TABLE_KEY;
import static util.TestProperties.DEFAULT_CONNECTION_INFO;

@Slf4j
public class SdkProtocolTest extends DriverTestBase {
	@Rule
	public SkipTestOnCondition skipTestOnCondition = new SkipTestOnCondition();

	@Test
	@ExecuteWhenSysVariableSet(SMALL_TABLE_KEY)
	public final void testReadContentSecure() throws IOException, AtsdException, GeneralSecurityException {
		final ContentDescription contentDescription = new ContentDescription(
				Location.SQL_ENDPOINT.getUrl(DEFAULT_CONNECTION_INFO), DEFAULT_CONNECTION_INFO,
				SELECT_TVE_CLAUSE + SMALL_TABLE + SELECT_LIMIT_1000, TestUtil.createStatementContext());
		IContentProtocol protocol = ProtocolFactory.create(SdkProtocolImpl.class, contentDescription);
        try (final InputStream inputStream = protocol.readContent(0)) {
			final Reader reader = new BufferedReader(new InputStreamReader(inputStream));
			assertNotNull(contentDescription.getJsonScheme());
			String[] nextLine;
			try (final CSVReader csvReader = new CSVReader(reader)) {
				while ((nextLine = csvReader.readNext()) != null) {
					String next = Arrays.toString(nextLine);
					assertThat(nextLine.length, is(greaterThan(0)));

					log.trace(next);
				}
			}
		}
	}
}