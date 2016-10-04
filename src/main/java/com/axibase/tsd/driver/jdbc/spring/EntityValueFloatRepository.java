package com.axibase.tsd.driver.jdbc.spring;

import com.axibase.tsd.driver.jdbc.spring.entity.EntityValueFloat;
import com.nurkiewicz.jdbcrepository.JdbcRepository;
import com.nurkiewicz.jdbcrepository.MissingRowUnmapper;
import com.nurkiewicz.jdbcrepository.RowUnmapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

@Repository("entityRepository")
public class EntityValueFloatRepository extends JdbcRepository<EntityValueFloat, Float> {

	public EntityValueFloatRepository() {
		super(ROW_MAPPER, new MissingRowUnmapper<EntityValueFloat>(), "");
	}

	public EntityValueFloatRepository(String table) {
		super(ROW_MAPPER, ROW_UNMAPPER, table);
	}

	public static final RowMapper<EntityValueFloat> ROW_MAPPER = new RowMapper<EntityValueFloat>() {
		@Override
		public EntityValueFloat mapRow(ResultSet rs, int rowNum) throws SQLException {
			return new EntityValueFloat(rs.getString("entity"), rs.getTimestamp("datetime"), rs.getFloat("value"));
		}
	};

	private static final RowUnmapper<EntityValueFloat> ROW_UNMAPPER = new RowUnmapper<EntityValueFloat>() {
		@Override
		public Map<String, Object> mapColumns(EntityValueFloat entity) {
			Map<String, Object> mapping = new LinkedHashMap<String, Object>();
			mapping.put("entity", entity.getEntity());
			mapping.put("datetime", entity.getDatetime());
			mapping.put("value", entity.getValue());
			return mapping;
		}
	};

}
