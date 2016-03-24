package com.axibase.tsd.driver.jdbc.spring;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.axibase.tsd.driver.jdbc.TestConstants;
import com.axibase.tsd.driver.jdbc.strategies.StrategyFactory;
import com.nurkiewicz.jdbcrepository.sql.SqlGenerator;
import com.zaxxer.hikari.HikariDataSource;

@Configuration
public class AtsdRepositoryConfig implements TestConstants {

	@Bean
	public SqlGenerator sqlGenerator() {
		return new AtsdSqlGenerator();
	}

	@Bean
	public DataSource dataSource() {
		String trustProp = System.getProperty("axibase.tsd.driver.jdbc.trust");
		String login = System.getProperty("axibase.tsd.driver.jdbc.username");
		String password = System.getProperty("axibase.tsd.driver.jdbc.password");
		String url = System.getProperty("axibase.tsd.driver.jdbc.url");
		final StringBuilder sb = new StringBuilder(JDBC_ATDS_URL_PREFIX).append(url);
		if (trustProp != null)
			sb.append(Boolean.valueOf(trustProp) ? TRUST_PARAMETER_IN_QUERY : UNTRUST_PARAMETER_IN_QUERY);
		String strategy = System.getProperty("axibase.tsd.driver.jdbc.strategy");
		if (strategy != null) {
			if (trustProp == null)
				sb.append(PARAM_SEPARATOR);
			sb.append(strategy.equalsIgnoreCase(StrategyFactory.FILE_STRATEGY) ? STRATEGY_FILE_PARAMETER
					: STRATEGY_STREAM_PARAMETER);
		}
		final HikariDataSource dataSource = new HikariDataSource();
		dataSource.setJdbcUrl(sb.toString());
		dataSource.setUsername(login);
		dataSource.setPassword(password);
		dataSource.setReadOnly(true);
		return dataSource;
	}

	@Bean
	public EntityValueDoubleRepository entityRepository() {
		String table = System.getProperty("axibase.tsd.driver.jdbc.metric.tiny");
		return new EntityValueDoubleRepository(table);
	}

}