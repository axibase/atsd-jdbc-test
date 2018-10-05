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

import com.axibase.tsd.driver.jdbc.content.ContentDescription;
import com.axibase.tsd.driver.jdbc.content.DataProvider;
import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import com.axibase.tsd.driver.jdbc.rules.ExecuteWhenSysVariableSet;
import com.axibase.tsd.driver.jdbc.rules.SkipTestOnCondition;
import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.Meta;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import util.TestUtil;

import java.util.List;

import static com.axibase.tsd.driver.jdbc.TestConstants.SELECT_ALL_CLAUSE;
import static com.axibase.tsd.driver.jdbc.TestConstants.SELECT_LIMIT_1000;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;
import static util.TableConstants.SMALL_TABLE;
import static util.TableConstants.SMALL_TABLE_KEY;
import static util.TestProperties.*;

@Slf4j
public class DataProviderTest {

    @Rule
    public SkipTestOnCondition skipTestOnCondition = new SkipTestOnCondition();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    @ExecuteWhenSysVariableSet(SMALL_TABLE_KEY)
    public final void testSecureByDefault() throws Exception {
        assertThat(testFetch(DEFAULT_CONNECTION_INFO, SMALL_TABLE), is(not(empty())));
    }

    @Test
    @ExecuteWhenSysVariableSet(SMALL_TABLE_KEY)
    public final void testCertificateTrusted() throws Exception {
        final AtsdConnectionInfo atsdConnectionInfo = new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)
                .withTrust("true")
                .composeConnectionInfo();
        assertThat(testFetch(atsdConnectionInfo, SMALL_TABLE), is(not(empty())));
    }

    @Test
    @ExecuteWhenSysVariableSet(SMALL_TABLE_KEY)
    public final void testCertificateUntrusted() throws Exception {
        final AtsdConnectionInfo atsdConnectionInfo = new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)
                .withTrust("false")
                .composeConnectionInfo();
        assertThat(testFetch(atsdConnectionInfo, SMALL_TABLE), is(not(empty())));
    }

    @Test
    @ExecuteWhenSysVariableSet(SMALL_TABLE_KEY)
    public final void testHttp() throws Exception {
        final AtsdConnectionInfo atsdConnectionInfo = new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)
                .withSecure("false")
                .composeConnectionInfo();
        assertThat(testFetch(atsdConnectionInfo, SMALL_TABLE), is(not(empty())));
    }

    private static List<List<Object>> testFetch(AtsdConnectionInfo atsdConnectionInfo, String table) throws Exception {
        try (DataProvider provider = new DataProvider(atsdConnectionInfo,
                SELECT_ALL_CLAUSE + table + SELECT_LIMIT_1000,
                TestUtil.createStatementContext(),Meta.StatementType.SELECT)) {
            provider.fetchData(1, 0);
            return provider.getStrategy().fetch(0, 1);
        }
    }

    @Test
    @ExecuteWhenSysVariableSet(SMALL_TABLE_KEY)
    public final void testGetContentDescription() throws Exception {
        try (DataProvider provider = new DataProvider(DEFAULT_CONNECTION_INFO,
                SELECT_ALL_CLAUSE + SMALL_TABLE + SELECT_LIMIT_1000,
                TestUtil.createStatementContext(), Meta.StatementType.SELECT)) {
            final ContentDescription contentDescription = provider.getContentDescription();
            assertNotNull(contentDescription);

            log.debug(contentDescription.getEndpoint());
            log.debug(contentDescription.getPostContent());
        }
    }

}
