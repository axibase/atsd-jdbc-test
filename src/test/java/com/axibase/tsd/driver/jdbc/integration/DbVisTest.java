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
package com.axibase.tsd.driver.jdbc.integration;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertNotNull;
import static com.axibase.tsd.driver.jdbc.util.TestProperties.*;

@Slf4j
public class DbVisTest {

    @Test
    public void checkDatabaseMetadata() throws ClassNotFoundException, SQLException {
        Class.forName("com.axibase.tsd.driver.jdbc.AtsdDriver");
        try (Connection connection = DriverManager.getConnection(DEFAULT_JDBC_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD)) {
            assertNotNull(connection);
            log.debug("Connection: {}", connection);
            final DatabaseMetaData metaData = connection.getMetaData();
            assertNotNull(metaData);
            log.debug("databaseProductName: {}", metaData.getDatabaseProductName());
            log.debug("databaseProductVersion: {}", metaData.getDatabaseProductVersion());
            log.debug("driverName: {}", metaData.getDriverName());
            log.debug("driverVersion: {}", metaData.getDriverVersion());

            try (final ResultSet typeInfo = metaData.getTypeInfo()) {
                assertNotNull(typeInfo);
                while (typeInfo.next()) {
                    log.debug("TypeInfo Name: {}", typeInfo.getString("TYPE_NAME"));
                    log.debug("TypeInfo Type: {}", typeInfo.getInt("DATA_TYPE"));
                    log.debug("TypeInfo Precision: {}", typeInfo.getInt("PRECISION"));
                    log.debug("TypeInfo CS: {}", typeInfo.getBoolean("CASE_SENSITIVE"));
                }
            }
            try (final ResultSet tableTypes = metaData.getTableTypes()) {
                assertNotNull(tableTypes);
                while (tableTypes.next()) {
                    log.debug("TableTypes: {}", tableTypes.getString(1));
                }
            }
            try (final ResultSet catalogs = metaData.getCatalogs()) {
                assertNotNull(catalogs);
                while (catalogs.next()) {
                    final String catalog = catalogs.getString(1);
                    log.debug("Catalog: {}", catalog);
                    final ResultSet schemas = metaData.getSchemas(catalog, null);
                    assertNotNull(schemas);
                    while (schemas.next()) {
                        log.debug("Schema: {}", schemas.getString(1));
                    }
                }
            }
        }
    }

}
