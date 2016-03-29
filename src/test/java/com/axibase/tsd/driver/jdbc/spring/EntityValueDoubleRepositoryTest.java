package com.axibase.tsd.driver.jdbc.spring;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.axibase.tsd.driver.jdbc.TestProperties;
import com.axibase.tsd.driver.jdbc.spring.entity.EntityValueDouble;

//@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class, classes = { AtsdRepositoryConfig.class })
public class EntityValueDoubleRepositoryTest extends TestProperties {

	@Resource
	private EntityValueDoubleRepository entityRepository;

	@Resource
	private DataSource dataSource;

	private JdbcOperations jdbc;

	@Before
	public void setUp() throws Exception {
		jdbc = new JdbcTemplate(dataSource);
	}

	@After
	public void tearDown() throws Exception {
		Connection connection = dataSource.getConnection();
		if (connection != null && !connection.isClosed())
			connection.close();
	}

	@Test
	public void testCount() {
		long count = entityRepository.count();
		assertTrue(TINY_TABLE_COUNT == -1 || count == TINY_TABLE_COUNT);
	}

	@Test
	public void testFindAll() {
		final PageRequest page = new PageRequest(0, 1000, Direction.DESC, "time", "value");
		final Page<EntityValueDouble> result = entityRepository.findAll(page);
		List<EntityValueDouble> list = result.getContent();
		final List<Map<String, Object>> map = jdbc.queryForList(
				String.format("SELECT entity, time, value FROM %s ORDER BY time, value DESC LIMIT 1000", TINY_TABLE));
		final Iterator<Map<String, Object>> iterator = map.iterator();
		while (iterator.hasNext()) {
			final Map<String, Object> next = iterator.next();
			final EntityValueDouble evd = new EntityValueDouble((String) next.get("entity"), (Long) next.get("time"),
					(Double) next.get("value"));
			assertTrue(list.contains(evd));
		}
	}

}
