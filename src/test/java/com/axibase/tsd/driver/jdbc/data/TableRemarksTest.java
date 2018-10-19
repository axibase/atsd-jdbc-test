package com.axibase.tsd.driver.jdbc.data;

import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
import io.qameta.allure.junit4.DisplayName;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static util.TestProperties.*;

@Slf4j
@RequiredArgsConstructor
@RunWith(Parameterized.class)
public class TableRemarksTest {
    private static Connection connection;
    private final String table;
    private final String remarks;

    @BeforeClass
    public static void prepareConnection() throws SQLException {
        if (connection == null) {
            connection = getConnectionWithTables("jvm_memory_used,atsd_%");
        }
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> tableRemarks() throws SQLException {
        prepareConnection();
        final DatabaseMetaData metaData = connection.getMetaData();
        try (final ResultSet tables = metaData.getTables(null, null, null, null)) {
            final List<Object[]> result = new ArrayList<>();
            while (tables.next()) {
                result.add(new String[]{tables.getString("TABLE_NAME"), tables.getString("REMARKS")});
            }
            return result;
        }
    }

    @Test
    @DisplayName("Test that table remarks metadata are valid")
    public void testTableRemarks() throws SQLException {
        log.info("Executing remarks query for table {}:\n{}", table, remarks);
        try (Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery(remarks)) {
             assertThat(resultSet.next(), is(true));
        }
    }

    private static Connection getConnectionWithTables(String value) throws SQLException {
        final String connectString = new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)
                .withTables(value)
                .composeConnectString();
        return DriverManager.getConnection(connectString, LOGIN_NAME, LOGIN_PASSWORD);
    }
}
