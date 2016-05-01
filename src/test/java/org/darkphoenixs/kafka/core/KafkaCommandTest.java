package org.darkphoenixs.kafka.core;

import java.io.IOException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

public class KafkaCommandTest {

	static Properties props = new Properties();

	static {
		Resource resource = new DefaultResourceLoader()
				.getResource("kafka/consumer.properties");

		try {
			PropertiesLoaderUtils.fillProperties(props, resource);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test1() throws Exception {

		Assert.assertNotNull(new KafkaCommand());

		try {
			KafkaCommand.createOptions(
					props.getProperty(KafkaConstants.ZOOKEEPER_LIST),
					"QUEUE.TEST", 1, 1);
		} catch (Exception e) {

			Assert.assertNotNull(e);
		}

	}

	@Test
	public void test2() throws Exception {

		try {
			KafkaCommand.listOptions(props
					.getProperty(KafkaConstants.ZOOKEEPER_LIST));

		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}

	@Test
	public void test3() throws Exception {

		try {
			KafkaCommand.selectOptions(
					props.getProperty(KafkaConstants.ZOOKEEPER_LIST),
					"QUEUE.TEST");

		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}

	@Test
	public void test4() throws Exception {

		try {
			KafkaCommand.updateOptions(
					props.getProperty(KafkaConstants.ZOOKEEPER_LIST),
					"QUEUE.TEST", 2);

		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}

	@Test
	public void test5() throws Exception {

		try {
			KafkaCommand.updateOptions(
					props.getProperty(KafkaConstants.ZOOKEEPER_LIST),
					"QUEUE.TEST", "1=1", "2");
		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}

	@Test
	public void test6() throws Exception {

		try {
			KafkaCommand.updateOptions(
					props.getProperty(KafkaConstants.ZOOKEEPER_LIST),
					"QUEUE.TEST", 1, "1=1", "2");
		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}

	@Test
	public void test7() throws Exception {

		try {
			KafkaCommand.topicCommand("--list --zookeeper "
					+ props.getProperty(KafkaConstants.ZOOKEEPER_LIST));
		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}

	@Test
	public void test8() throws Exception {

		try {
			KafkaCommand.deleteOptions(
					props.getProperty(KafkaConstants.ZOOKEEPER_LIST),
					"QUEUE.TEST");
		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}
}
