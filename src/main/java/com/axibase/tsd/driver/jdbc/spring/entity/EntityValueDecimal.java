package com.axibase.tsd.driver.jdbc.spring.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class EntityValueDecimal extends Entity<BigDecimal> {
	private static final long serialVersionUID = -2193339029893527586L;

	public EntityValueDecimal(String entity, Timestamp datetime, BigDecimal value) {
		super(entity, datetime, value);
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return super.equals(obj);
	}

}