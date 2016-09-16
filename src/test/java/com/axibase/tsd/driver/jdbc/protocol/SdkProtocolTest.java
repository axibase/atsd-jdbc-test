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
package com.axibase.tsd.driver.jdbc.protocol;

import static com.axibase.tsd.driver.jdbc.TestConstants.*;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.security.GeneralSecurityException;
import java.util.Arrays;

import com.axibase.tsd.driver.jdbc.DriverConstants;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.axibase.tsd.driver.jdbc.TestProperties;
import com.axibase.tsd.driver.jdbc.content.ContentDescription;
import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.axibase.tsd.driver.jdbc.intf.IContentProtocol;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.opencsv.CSVReader;

public class SdkProtocolTest extends TestProperties {
	private static final LoggingFacade logger = LoggingFacade.getLogger(SdkProtocolTest.class);

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testReadContentSecure() throws IOException, AtsdException, GeneralSecurityException {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		String[] params = TRUST_URL != null ? new String[] {
				PARAM_SEPARATOR + DriverConstants.TRUST_PARAM_NAME + '=' + TRUST_URL}
				: new String[0];
		final ContentDescription cd = new ContentDescription(HTTP_ATDS_URL,
				SELECT_TVE_CLAUSE + SMALL_TABLE + SELECT_LIMIT_1000, LOGIN_NAME, LOGIN_PASSWORD, params);
		IContentProtocol impl = ProtocolFactory.create(SdkProtocolImpl.class, cd);
		try (final InputStream is = impl.readContent()) {
			final Reader reader = new BufferedReader(new InputStreamReader(is));
			assertNotNull(cd.getJsonScheme());
			String[] nextLine;
			try (final CSVReader csvReader = new CSVReader(reader)) {
				while ((nextLine = csvReader.readNext()) != null) {
					String next = Arrays.toString(nextLine);
					assertNotNull(next);
					if (logger.isTraceEnabled())
						logger.trace(next);
				}
			}
		}
	}

}