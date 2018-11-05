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

import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.axibase.tsd.driver.jdbc.util.TestProperties;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Stopwatch;
import org.junit.runner.Description;
import com.axibase.tsd.driver.jdbc.rules.ExecuteWhenSysVariableSet;
import com.axibase.tsd.driver.jdbc.rules.SkipTestOnCondition;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static com.axibase.tsd.driver.jdbc.TestConstants.*;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Slf4j
public class DataFromProvidedTablesTest extends AbstractDataTest {

    @Rule
    public SkipTestOnCondition skipTestOnCondition = new SkipTestOnCondition();

    @Rule
    public Stopwatch stopwatch = new Stopwatch() {
        @Override
        protected void succeeded(long nanos, Description description) {
            log.info("Test {} finished in {} ms ", description, TimeUnit.NANOSECONDS.toMillis(nanos));
        }

        @Override
        protected void failed(long nanos, Throwable e, Description description) {
            log.info("Test {} finished in {} ms ", description, TimeUnit.NANOSECONDS.toMillis(nanos));
        }
    };

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.TINY_TABLE_KEY)
    @ExecuteWhenSysVariableSet(TestProperties.TINY_TABLE_COUNT_KEY)
    public final void tinyRemoteStatement() throws AtsdException, SQLException {
        long count = checkRemoteStatement(SELECT_ALL_CLAUSE + TestProperties.TINY_TABLE, NO_RETRIES);
        assertThat(count, is(TestProperties.TINY_TABLE_COUNT));
        log.debug("size of '{}' is {}", TestProperties.TINY_TABLE, count);
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.SMALL_TABLE_KEY)
    public final void smallRemoteStatement() throws AtsdException, SQLException {
        long count = checkRemoteStatement(SELECT_ALL_CLAUSE + TestProperties.SMALL_TABLE, NO_RETRIES);
        log.debug("size of '{}' is {}", TestProperties.SMALL_TABLE, count);
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.MEDIUM_TABLE_KEY)
    public final void mediumRemoteStatement() throws AtsdException, SQLException {
        long count = checkRemoteStatement(SELECT_ALL_CLAUSE + TestProperties.MEDIUM_TABLE, NO_RETRIES);
        log.debug("size of '{}' is {}", TestProperties.MEDIUM_TABLE, count);
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.LARGE_TABLE_KEY)
    public final void largeRemoteStatement() throws AtsdException, SQLException {
        long count = checkRemoteStatement(SELECT_ALL_CLAUSE + TestProperties.LARGE_TABLE, NO_RETRIES);
        log.debug("size of '{}' is {}", TestProperties.LARGE_TABLE, count);
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.HUGE_TABLE_KEY)
    public final void hugeRemoteStatement() throws AtsdException, SQLException {
        long count = checkRemoteStatement(SELECT_ALL_CLAUSE + TestProperties.HUGE_TABLE, NO_RETRIES);
        log.debug("size of '{}' is {}", TestProperties.HUGE_TABLE, count);
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.TINY_TABLE_KEY)
    @ExecuteWhenSysVariableSet(TestProperties.TINY_TABLE_COUNT_KEY)
    public final void testRemoteStatementWithFields() throws AtsdException, SQLException {
        long count = checkRemoteStatement(SELECT_TVE_CLAUSE + TestProperties.TINY_TABLE, NO_RETRIES);
        assertThat(count, is(TestProperties.TINY_TABLE_COUNT));
        log.debug("size of '{}' is {}", TestProperties.TINY_TABLE, count);
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.TINY_TABLE_KEY)
    @ExecuteWhenSysVariableSet(TestProperties.TINY_TABLE_COUNT_KEY)
    public final void testRemoteStatementWithDates() throws AtsdException, SQLException {
        long count = checkRemoteStatement(SELECT_DVE_CLAUSE + TestProperties.TINY_TABLE, NO_RETRIES);
        assertThat(count, is(TestProperties.TINY_TABLE_COUNT));
        log.debug("size of '{}' is {}", TestProperties.TINY_TABLE, count);
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.TINY_TABLE_KEY)
    @ExecuteWhenSysVariableSet(TestProperties.TINY_TABLE_COUNT_KEY)
    public final void testRemotePreparedStatement() throws AtsdException, SQLException {
        long count = checkRemotePreparedStatementNoArgs(SELECT_DVE_CLAUSE + TestProperties.TINY_TABLE);
        assertThat(count, is(TestProperties.TINY_TABLE_COUNT));
        log.debug("size of '{}' is {}", TestProperties.TINY_TABLE, count);
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.SMALL_TABLE_KEY)
    public final void smallRemoteStatementTwice() throws AtsdException, SQLException {
        long count = checkRemoteStatement(SELECT_ALL_CLAUSE + TestProperties.SMALL_TABLE, 2);
        log.debug("size of '{}' is {}", TestProperties.SMALL_TABLE, count);
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.SMALL_TABLE_KEY)
    public final void testRemoteStatementsOnSmall() throws AtsdException, SQLException {
        checkRemoteStatementWithLimits(SELECT_ALL_CLAUSE + TestProperties.SMALL_TABLE + SELECT_LIMIT_1000, 1001, 10001);
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.MEDIUM_TABLE_KEY)
    public final void testRemoteStatementsOnMedium() throws AtsdException, SQLException {
        checkRemoteStatementWithLimits(SELECT_ALL_CLAUSE + TestProperties.MEDIUM_TABLE + SELECT_LIMIT_100000, 10001, 100001);
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.LARGE_TABLE_KEY)
    public final void testRemoteStatementsOnLarge() throws AtsdException, SQLException {
        checkRemoteStatementWithLimits(SELECT_ALL_CLAUSE + TestProperties.LARGE_TABLE + SELECT_LIMIT_100000, 100001, 1000001);
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.TWO_TABLES_KEY)
    public final void remoteStatementWithDifferentResultSets() throws AtsdException, SQLException {
        checkRemoteStatementWithDifferentResultSets();
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.TWO_TABLES_KEY)
    public final void remoteStatementWithTraversingSimultaneously()
            throws AtsdException, SQLException, InterruptedException {
        checkStatementWithTraversingSimultaneously();
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.TINY_TABLE_KEY)
    @ExecuteWhenSysVariableSet(TestProperties.TINY_TABLE_COUNT_KEY)
    public final void testRemoteStatementsWithLimits() throws AtsdException, SQLException {
        long count = checkRemoteStatementWithLimits(SELECT_ALL_CLAUSE + TestProperties.TINY_TABLE, 101, 10001);
        assertThat(count, is(TestProperties.TINY_TABLE_COUNT));
    }

    @Test(expected = UnsupportedOperationException.class)
    @ExecuteWhenSysVariableSet(TestProperties.TINY_TABLE_KEY)
    public final void smallRemoteStatementWithAbsPos() throws AtsdException, SQLException {
        try (final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery(SELECT_ALL_CLAUSE + TestProperties.TINY_TABLE)) {
            resultSet.absolute(100);
            executeAndReturnNumberOfLinesHelper(resultSet, sqlWarning -> {});
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    @ExecuteWhenSysVariableSet(TestProperties.TINY_TABLE_KEY)
    public final void smallRemoteStatementWithRelPos() throws AtsdException, SQLException {
        try (final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery(SELECT_ALL_CLAUSE + TestProperties.TINY_TABLE)) {
            resultSet.relative(100);
            executeAndReturnNumberOfLinesHelper(resultSet, sqlWarning -> {});
        }
    }

    @Test
    @ExecuteWhenSysVariableSet(TestProperties.WRONG_TABLE_KEY)
    public final void wrongRemoteStatement() throws AssertionError, AtsdException, SQLException {
        try (final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery(SELECT_ALL_CLAUSE + TestProperties.WRONG_TABLE)) {
            executeAndReturnNumberOfLinesHelper(resultSet,
                    sqlWarning -> assertThat(sqlWarning.getMessage(), endsWith("not found")));
        }
    }

    static long checkRemoteStatement(String sql, int retries) throws AtsdException, SQLException {
        try (final Statement statement = connection.createStatement()) {
            long count = 0;
            for (int i = 0; i < retries; i++) {
                count = executeAndReturnNumberOfLines(statement, sql);
            }
            return count;
        }
    }

}
