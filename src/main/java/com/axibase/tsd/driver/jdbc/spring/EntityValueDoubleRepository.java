package com.axibase.tsd.driver.jdbc.spring;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.axibase.tsd.driver.jdbc.spring.entity.EntityValueDouble;
import com.nurkiewicz.jdbcrepository.JdbcRepository;
import com.nurkiewicz.jdbcrepository.MissingRowUnmapper;
import com.nurkiewicz.jdbcrepository.RowUnmapper;

@Repository("entityRepository")
public class EntityValueDoubleRepository extends JdbcRepository<EntityValueDouble, Double> {

	public EntityValueDoubleRepository() {
		super(ROW_MAPPER, new MissingRowUnmapper<EntityValueDouble>(), "");
	}

	public EntityValueDoubleRepository(String table) {
		super(ROW_MAPPER, ROW_UNMAPPER, table);
	}

	public static final RowMapper<EntityValueDouble> ROW_MAPPER = new RowMapper<EntityValueDouble>() {
		@Override
		public EntityValueDouble mapRow(ResultSet rs, int rowNum) throws SQLException {
			return new EntityValueDouble(rs.getString("entity"), rs.getLong("time"), rs.getDouble("value"));
		}
	};

	private static final RowUnmapper<EntityValueDouble> ROW_UNMAPPER = new RowUnmapper<EntityValueDouble>() {
		@Override
		public Map<String, Object> mapColumns(EntityValueDouble entity) {
			Map<String, Object> mapping = new LinkedHashMap<String, Object>();
			mapping.put("entity", entity.getEntity());
			mapping.put("time", entity.getTime());
			mapping.put("value", entity.getValue());
			return mapping;
		}
	};

}
