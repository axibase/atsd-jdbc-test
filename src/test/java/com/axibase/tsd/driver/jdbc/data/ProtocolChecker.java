package com.axibase.tsd.driver.jdbc.data;

import com.axibase.tsd.driver.jdbc.content.ContentDescription;
import com.axibase.tsd.driver.jdbc.enums.Location;
import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;
import com.axibase.tsd.driver.jdbc.rules.ExecuteWhenSysVariableSet;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import util.TestUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;

import static com.axibase.tsd.driver.jdbc.TestConstants.SELECT_LIMIT_1000;
import static com.axibase.tsd.driver.jdbc.TestConstants.SELECT_TVE_CLAUSE;
import static org.junit.Assert.assertNotNull;
import static util.TableConstants.SMALL_TABLE;
import static util.TableConstants.SMALL_TABLE_KEY;
import static util.TestProperties.DEFAULT_CONNECTION_INFO;

@Slf4j
@PowerMockIgnore({ "javax.net.ssl.*" })
@RunWith(PowerMockRunner.class)
@PrepareForTest(SdkProtocolImpl.class)
public class ProtocolChecker {
	private SdkProtocolImpl protocol;

	@Before
    public void setUp() throws Exception {
		final ContentDescription contentDescription = new ContentDescription(
				Location.SQL_ENDPOINT.getUrl(DEFAULT_CONNECTION_INFO), DEFAULT_CONNECTION_INFO,
				SELECT_TVE_CLAUSE + SMALL_TABLE + SELECT_LIMIT_1000, TestUtil.createStatementContext());
		this.protocol = PowerMockito.spy(new SdkProtocolImpl(contentDescription));
	}

	@After
	public void tearDown() throws Exception {
		this.protocol.close();
	}

	@Test
	@ExecuteWhenSysVariableSet(SMALL_TABLE_KEY)
	public final void testPost() throws IOException, AtsdException, GeneralSecurityException {
		InputStream inputStream = this.protocol.readContent(0);
		assertNotNull(inputStream);
		if (log.isTraceEnabled()) {
			printContent(inputStream);
		}
	}

	@Test
	@ExecuteWhenSysVariableSet(SMALL_TABLE_KEY)
	public final void testGet() throws IOException, AtsdException, GeneralSecurityException {
		InputStream inputStream = this.protocol.readInfo();
		assertNotNull(inputStream);
		if (log.isTraceEnabled()) {
			printContent(inputStream);
		}
	}

	private void printContent(final InputStream inputStream) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			log.trace(line);
		}
	}

}