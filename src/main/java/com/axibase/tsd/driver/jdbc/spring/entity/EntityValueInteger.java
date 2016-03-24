package com.axibase.tsd.driver.jdbc.spring.entity;

public class EntityValueInteger extends Entity<Integer> {
	private static final long serialVersionUID = 6554309783286773927L;

	public EntityValueInteger(String entity, Long time, Integer value) {
		super(entity, time, value);
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