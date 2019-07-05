package com.axibase.tsd.driver.jdbc.spring.entity;

import java.sql.Timestamp;

public class EntityValueShort extends Entity<Short> {
	private static final long serialVersionUID = -7563334205049438473L;

	public EntityValueShort(String entity, Timestamp datetime, Short value) {
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