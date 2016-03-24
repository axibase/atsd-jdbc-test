package com.axibase.tsd.driver.jdbc.spring.entity;

import org.springframework.data.domain.Persistable;

public class Entity<T extends Number> implements Persistable<T> {
	private static final long serialVersionUID = 3165628205392048453L;
	private String entity;
	private Long time;
	private T value;
	private String tags;

	public Entity(String entity, Long time, T value) {
		this.entity = entity;
		this.time = time;
		this.value = value;
	}

	@Override
	public T getId() {
		return null;
	}

	@Override
	public boolean isNew() {
		return false;
	}

	public String getEntity() {
		return entity;
	}

	public Long getTime() {
		return time;
	}

	public T getValue() {
		return value;
	}

	public String getTags() {
		return tags;
	}

	public void setEntity(String entity) {
		this.entity = entity;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public void setValue(T value) {
		this.value = value;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((entity == null) ? 0 : entity.hashCode());
		result = prime * result + ((tags == null) ? 0 : tags.hashCode());
		result = prime * result + ((time == null) ? 0 : time.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Entity<?> other = (Entity<?>) obj;
		if (entity == null) {
			if (other.entity != null)
				return false;
		} else if (!entity.equals(other.entity))
			return false;
		if (tags == null) {
			if (other.tags != null)
				return false;
		} else if (!tags.equals(other.tags))
			return false;
		if (time == null) {
			if (other.time != null)
				return false;
		} else if (!time.equals(other.time))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Entity [entity=" + entity + ", time=" + time + ", value=" + value + ", tags=" + tags + "]";
	}

}
