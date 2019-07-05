package com.axibase.tsd.driver.jdbc.spring;

import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
import com.axibase.tsd.driver.jdbc.util.TestProperties;
import com.nurkiewicz.jdbcrepository.sql.SqlGenerator;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class AtsdRepositoryConfig {

	@Bean
	public SqlGenerator sqlGenerator() {
		return new AtsdSqlGenerator();
	}

	@Bean
	public DataSource dataSource() {
		final String login = TestProperties.LOGIN_NAME;
		final String password = TestProperties.LOGIN_PASSWORD;
		final String url = TestProperties.HTTP_ATSD_URL;
		final String strategy = TestProperties.READ_STRATEGY;

		final HikariDataSource dataSource = new HikariDataSource();
		final String jdbcUrl = new ConnectStringComposer(url, login, password).withStrategy(strategy).composeConnectString();
		dataSource.setJdbcUrl(jdbcUrl);
		dataSource.setUsername(login);
		dataSource.setPassword(password);
		dataSource.setReadOnly(true);
		return dataSource;
	}

	@Bean
	public EntityValueFloatRepository entityRepository() {
		final String table = TestProperties.TINY_TABLE;
		return new EntityValueFloatRepository(table);
	}

}