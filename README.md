[![Dependency Status](https://www.versioneye.com/user/projects/57b45deaf0b3bb00487de3a7/badge.svg?style=flat)](https://www.versioneye.com/user/projects/57b45deaf0b3bb00487de3a7)
# Integration tests for JDBC driver

## Requirements

* Java 1.8

## Run tests

The tests are built automatically by Travis CI, but you also can run it manually:

- uncomment last 3 rows at ./run_test.sh

```bash
mvn clean test -B
docker rm -fv jdbc_test
docker rmi atsd:jdbc-test
```
> Note a set of test properties already specified, i.e. `src/test/resources/dev.properties` filled automatically

- launch run_test.sh under root directory of atsd-jdbc-test 

```bash
./run_test.sh HTTPS_PORT TCP_PORT
```
Parameters `HTTPS PORT`, `TCP PORT` are not required, if they are skipped script runs search of free ports.
> Before the start of the tests run_test.sh launches ATSD instance with pre-installed m_small (100 samples), m_large (500 000 samples) metrics and user axibase:axibase

## Redirect stdout and stderr to allure report

> Works only for tests with OutputLogsToAllure rule enabled

Replace `mvn clean test -B` with `mvn clean test -Doutput.redirect.allure=true` at run_test.sh

```bash
$ mvn clean test -Doutput.redirect.allure=true
```
## Generate report and run on localhost:1234

Replace `mvn clean test -B` with `mvn allure:report jetty:run -Djetty.port=1234` at run_test.sh

```bash
$ mvn allure:report jetty:run -Djetty.port=1234
```

![](images/allure_fullscreen.png)

## Custom JUnit Rules
* `OutputLogsToAllure(boolean enable)` -- redirect logging to allure attachments
* `SkipTestOnCondition` -- allows to skip test if required system variable is not set. Tests with required variables should be annotated with the `ExecuteWhenSysVariableSet` annotation which has a string parameter -- required system variable. This annotation is repeatable.

## Exposed parameters

The following parameters may be specified on test execution, for example:

```bash
mvn clean test -Dinsert.wait=500
```

Parameter | Supported Values | Example Value | Description
------------|-------------|------------|-----------
output.redirect.allure | true or not set | true | Redirect per-test execution logging to allure attachment
allure.link.issue.pattern | string with `{}` placeholder | https://localhost/redmine/issues/{} | Pattern for generating links to bugtracking system
insert.wait | integral number, default is 1000 | 5000 | waiting timeout in ms between insert and subsequent select statement 

