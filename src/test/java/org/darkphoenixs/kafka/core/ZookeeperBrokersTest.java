package org.darkphoenixs.kafka.core;

import java.io.IOException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

public class ZookeeperBrokersTest {

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
	public void test() throws Exception {

		ZookeeperHosts zooHosts = new ZookeeperHosts(
				props.getProperty(KafkaConstants.ZOOKEEPER_LIST), "QUEUE.TEST");

		final ZookeeperBrokers brokers = new ZookeeperBrokers(zooHosts);

		brokers.brokerPath();

		brokers.partitionPath();

		Assert.assertEquals(
				"localhost:9092",
				brokers.getBrokerHost("{\"host\":\"localhost\", \"jmx_port\":9999, \"port\":9092, \"version\":1 }"
						.getBytes("UTF-8")));

		try {
			brokers.getBrokerHost("1".getBytes());
		} catch (Exception e) {
			Assert.assertNotNull(e);
		}

		Thread thread1 = new Thread(new Runnable() {

			@Override
			public void run() {

				brokers.getBrokerInfo();
			}
		});

		thread1.setDaemon(true);

		thread1.start();

		Thread thread2 = new Thread(new Runnable() {

			@Override
			public void run() {

				brokers.getNumPartitions();
			}
		});

		thread2.setDaemon(true);

		thread2.start();

		Thread thread3 = new Thread(new Runnable() {

			@Override
			public void run() {

				brokers.getLeaderFor(0);
			}
		});

		thread3.setDaemon(true);

		thread3.start();

		brokers.close();
	}
}
