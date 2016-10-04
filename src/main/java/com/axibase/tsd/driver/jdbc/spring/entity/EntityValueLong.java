package com.axibase.tsd.driver.jdbc.spring.entity;

import java.sql.Timestamp;

public class EntityValueLong extends Entity<Long> {
	private static final long serialVersionUID = -7516982588762842689L;

	public EntityValueLong(String entity, Timestamp datetime, Long value) {
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