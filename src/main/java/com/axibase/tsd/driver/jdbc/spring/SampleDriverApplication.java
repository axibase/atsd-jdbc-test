package com.axibase.tsd.driver.jdbc.spring;

import java.util.Arrays;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

@SpringBootApplication
@ContextConfiguration(loader = AnnotationConfigContextLoader.class, classes = { AtsdRepositoryConfig.class })
public class SampleDriverApplication {

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(SampleDriverApplication.class);
		app.setBannerMode(Banner.Mode.OFF);
		ApplicationContext ctx = app.run(args);
		assert (Arrays.asList(ctx.getBeanDefinitionNames()).contains("entityRepository"));
	}

}
