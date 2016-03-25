[![Dependency Status](https://www.versioneye.com/user/projects/56f5282e35630e003e0a85e3/badge.svg?style=flat)](https://www.versioneye.com/user/projects/56f5282e35630e003e0a85e3)

# Integration tests for JDBC driver

## Requirements

* Java 1.7 and later

## Maven

```
$ mvn clean test
```

To run tests, you have to choose (or create) your own ATSD metrics. A test phase requires a set of test properties listed below. The first five parameters are mandatory.

```
* -Daxibase.tsd.driver.jdbc.url=<ATSD_URL | http, https | >
* -Daxibase.tsd.driver.jdbc.username=<ATSD_LOGIN>
* -Daxibase.tsd.driver.jdbc.password=<ATSD_PASSWORD>
* -Daxibase.tsd.driver.jdbc.metric.tiny=<TEST_METRIC_NAME>
* -Daxibase.tsd.driver.jdbc.metric.tiny.count=<TEST_METRIC_EXPECTED_RECORDS>
* -Daxibase.tsd.driver.jdbc.trust=<TRUST_SERVER_CERTIFICATE>
* -Daxibase.tsd.driver.jdbc.strategy=<STORE_STRATEGY | file, stream | >
```
