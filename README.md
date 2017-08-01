[![Dependency Status](https://www.versioneye.com/user/projects/57b45deaf0b3bb00487de3a7/badge.svg?style=flat)](https://www.versioneye.com/user/projects/57b45deaf0b3bb00487de3a7)
# Integration tests for JDBC driver

## Requirements

* Java 1.8

## Run tests

```
$ mvn clean test
```

## Redirect stdout and stderr to allure report

> Works only for tests with OutputLogsToAllure rule enabled

```
$ mvn clean test -Doutput.redirect.allure=true
```
## Generate report and run on localhost:1234

```
$ mvn allure:report jetty:run -Djetty.port=1234
```

![](images/allure_fullscreen.png)

## Exposed parameters

The following parameters may be specified to control test execution:

Parameter | Supported Values | Example | Description
------------|-------------|------------|-----------
output.redirect.allure | true or not set | -Doutput.redirect.allure=true | Redirect per-test execution logging to allure attachment
allure.link.issue.pattern | string with `{}` placeholder | -Dallure.link.issue.pattern=https://localhost/redmine/issues/{} | Pattern for generating links to bugtracking system
insert.wait | integral number | -Dinsert.wait=5000 | waiting timeout in ms between insert and subsequent select statement 

## Custom JUnit Rules
* `OutputLogsToAllure(boolean enable)` -- redirect logging to allure attachments
* `SkipTestOnCondition` -- allows to skip test if required system variable is not set. Tests with required variables should be annotated with the `ExecuteWhenSysVariableSet` annotation which has a string parameter -- required system variable. This annotation is repeatable.

To run tests, you have to choose (or create) your own ATSD metrics. A test phase requires a set of test properties listed below. You need to fill the file `src/test/resources/dev.properties` with your metrics. The following properties are mandatory: 

```
axibase.tsd.driver.jdbc.url=<ATSD_URL | host:port | >
axibase.tsd.driver.jdbc.username=<ATSD_LOGIN>
axibase.tsd.driver.jdbc.password=<ATSD_PASSWORD>
axibase.tsd.driver.jdbc.metric.tiny=<TEST_METRIC_NAME>
axibase.tsd.driver.jdbc.metric.tiny.count=<TEST_METRIC_EXPECTED_RECORDS>
```
