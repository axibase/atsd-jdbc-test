package com.axibase.tsd.driver.jdbc.spring.entity;

import java.sql.Timestamp;

public class EntityValueDouble extends Entity<Double> {
	private static final long serialVersionUID = 8176116599219606350L;

	public EntityValueDouble(String entity, Timestamp datetime, Double value) {
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