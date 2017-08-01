package com.axibase.tsd.driver.jdbc.spring;

import com.axibase.tsd.driver.jdbc.spring.entity.EntityValueFloat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.List;

@Component
@Slf4j
public class SampleDriverRunner implements CommandLineRunner {
	@Resource
	private EntityValueFloatRepository entityRepository;

	@Override
	public void run(String... args) throws Exception {
		if (log.isInfoEnabled())
			log.info("This application shows how to use JDBC driver in Spring environment");
		String property = System.getProperty("axibase.tsd.driver.jdbc.metric.tiny.count");
		int expected = !StringUtils.isEmpty(property) ? Integer.parseInt(property) : -1;
		Assert.notNull(entityRepository);
		long counted = entityRepository.count();
		if (log.isInfoEnabled())
			log.info("Count: " + counted);
		Assert.isTrue(counted == expected);
		final PageRequest page = new PageRequest(0, 1000, Direction.DESC, "time", "value");
		final Page<EntityValueFloat> result = entityRepository.findAll(page);
		List<EntityValueFloat> list = result.getContent();
		Assert.notEmpty(list);
		if (log.isInfoEnabled())
			log.info("Size: " + list.size());
		Assert.isTrue(list.size() == expected);
		if (log.isInfoEnabled())
			log.info("List: " + list.toString());
	}

}