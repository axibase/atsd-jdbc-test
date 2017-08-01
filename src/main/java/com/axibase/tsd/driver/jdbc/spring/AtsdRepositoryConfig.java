package com.axibase.tsd.driver.jdbc.spring;

import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
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
		final String login = System.getProperty("axibase.tsd.driver.jdbc.username");
		final String password = System.getProperty("axibase.tsd.driver.jdbc.password");
		final String url = System.getProperty("axibase.tsd.driver.jdbc.url");
		final String strategy = System.getProperty("axibase.tsd.driver.jdbc.strategy");

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
		final String table = System.getProperty("axibase.tsd.driver.jdbc.metric.tiny");
		return new EntityValueFloatRepository(table);
	}

}