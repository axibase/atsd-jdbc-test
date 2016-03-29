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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.axibase.tsd.driver.jdbc.TestProperties;
import com.axibase.tsd.driver.jdbc.content.ContentDescription;
import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;

@PowerMockIgnore({ "javax.net.ssl.*" })
@RunWith(PowerMockRunner.class)
@PrepareForTest(SdkProtocolImpl.class)
public class ProtocolChecker extends TestProperties {
	private static final LoggingFacade logger = LoggingFacade.getLogger(ProtocolChecker.class);
	private SdkProtocolImpl impl;

	@Before
	public void setUp() throws Exception {
		String[] params = TRUST_URL != null ? new String[] {
				TRUST_URL.booleanValue() ? ContentDescription.TRUST_PARAM_TRUE : ContentDescription.TRUST_PARAM_FALSE }
				: new String[0];
		ContentDescription cd = new ContentDescription(HTTP_ATDS_URL,
				SELECT_TVE_CLAUSE + SMALL_TABLE + SELECT_LIMIT_1000, LOGIN_NAME, LOGIN_PASSWORD, params);
		this.impl = PowerMockito.spy(new SdkProtocolImpl(cd));
	}

	@After
	public void tearDown() throws Exception {
		this.impl.close();
	}

	@Test
	public final void testPost() throws IOException, AtsdException, GeneralSecurityException {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		InputStream is = this.impl.executeRequest(SdkProtocolImpl.POST_METHOD);
		assertNotNull(is);
		if (logger.isTraceEnabled()) {
			printContent(is);
		}
	}

	@Test
	public final void testGet() throws IOException, AtsdException, GeneralSecurityException {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		InputStream is = this.impl.executeRequest(SdkProtocolImpl.GET_METHOD);
		assertNotNull(is);
		if (logger.isTraceEnabled()) {
			printContent(is);
		}
	}

	@Test
	public final void testHead() throws IOException, AtsdException, GeneralSecurityException {
		if (StringUtils.isEmpty(SMALL_TABLE))
			return;
		InputStream is = this.impl.executeRequest(SdkProtocolImpl.HEAD_METHOD);
		assertNull(is);
	}

	private void printContent(final InputStream inputStream) throws IOException {
		BufferedReader is = new BufferedReader(new InputStreamReader(inputStream));
		String line;
		while ((line = is.readLine()) != null) {
			logger.trace(line);
		}
	}

}