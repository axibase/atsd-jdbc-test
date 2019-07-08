package com.axibase.tsd.driver.jdbc.spring.entity;

import java.sql.Timestamp;

public class EntityValueFloat extends Entity<Float> {
	private static final long serialVersionUID = 2082760089730598828L;

	public EntityValueFloat(String entity, Timestamp datetime, Float value) {
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