package org.darkphoenixs.kafka.core;

import java.io.IOException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

public class ZookeeperHostsTest {

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

		Assert.assertEquals(KafkaConstants.DEFAULT_ZK_ROOT,
				zooHosts.getBrokerZkPath());

		Assert.assertEquals(props.getProperty(KafkaConstants.ZOOKEEPER_LIST),
				zooHosts.getBrokerZkStr());

		Assert.assertEquals("QUEUE.TEST", zooHosts.getTopic());

		ZookeeperHosts zooHosts1 = new ZookeeperHosts(
				props.getProperty(KafkaConstants.ZOOKEEPER_LIST),
				KafkaConstants.DEFAULT_ZK_ROOT, "QUEUE.TEST");

		Assert.assertEquals(KafkaConstants.DEFAULT_REFRESH_FRE_SEC,
				zooHosts1.getRefreshFreqSecs());

		zooHosts1.setRefreshFreqSecs(0);

	}
}
