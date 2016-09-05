package com.axibase.tsd.driver.jdbc.spring;

import java.util.List;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.stereotype.Component;

import com.axibase.tsd.driver.jdbc.TestConstants;
import com.axibase.tsd.driver.jdbc.spring.entity.EntityValueDouble;
import org.springframework.util.Assert;

@Component
public class SampleDriverRunner implements CommandLineRunner, TestConstants {
	private static final Logger logger = LoggerFactory.getLogger(SampleDriverRunner.class);

	@Resource
	private EntityValueDoubleRepository entityRepository;

	@Override
	public void run(String... args) throws Exception {
		if (logger.isInfoEnabled())
			logger.info("This application shows how to use JDBC driver in Spring environment");
		String property = System.getProperty("axibase.tsd.driver.jdbc.metric.tiny.count");
		int expected = !StringUtils.isEmpty(property) ? Integer.parseInt(property) : -1;
		Assert.notNull(entityRepository);
		long counted = entityRepository.count();
		if (logger.isInfoEnabled())
			logger.info("Count: " + counted);
		Assert.isTrue(counted == expected);
		final PageRequest page = new PageRequest(0, 1000, Direction.DESC, "time", "value");
		final Page<EntityValueDouble> result = entityRepository.findAll(page);
		List<EntityValueDouble> list = result.getContent();
		Assert.notEmpty(list);
		if (logger.isInfoEnabled())
			logger.info("Size: " + list.size());
		Assert.isTrue(list.size() == expected);
		if (logger.isInfoEnabled())
			logger.info("List: " + list.toString());
	}

}