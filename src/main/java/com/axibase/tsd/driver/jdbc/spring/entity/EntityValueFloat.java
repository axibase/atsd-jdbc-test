package com.axibase.tsd.driver.jdbc.spring.entity;

public class EntityValueFloat extends Entity<Float> {
	private static final long serialVersionUID = 2082760089730598828L;

	public EntityValueFloat(String entity, Long time, Float value) {
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