package com.axibase.tsd.driver.jdbc.spring;

import org.springframework.data.domain.Pageable;

import com.nurkiewicz.jdbcrepository.sql.SqlGenerator;

public class AtsdSqlGenerator extends SqlGenerator {
	public AtsdSqlGenerator() {
	}

	public AtsdSqlGenerator(String allColumnsClause) {
		super(allColumnsClause);
	}

	@Override
	protected String limitClause(Pageable page) {
		return "";
	}

}
