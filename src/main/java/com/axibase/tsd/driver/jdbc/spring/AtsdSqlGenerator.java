package com.axibase.tsd.driver.jdbc.spring;

import com.nurkiewicz.jdbcrepository.TableDescription;
import com.nurkiewicz.jdbcrepository.sql.SqlGenerator;

public class AtsdSqlGenerator extends SqlGenerator {
	public AtsdSqlGenerator() {
	}

	public AtsdSqlGenerator(String allColumnsClause) {
		super(allColumnsClause);
	}

	@Override
	public String count(TableDescription table) {
		return SELECT + "COUNT(*) " + FROM + '"' + table.getFromClause() + '"';
	}
}
