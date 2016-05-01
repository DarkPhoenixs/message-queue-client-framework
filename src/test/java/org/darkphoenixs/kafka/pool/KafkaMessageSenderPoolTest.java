package org.darkphoenixs.kafka.pool;

import java.io.IOException;
import java.util.Properties;

import org.darkphoenixs.kafka.core.KafkaConstants;
import org.darkphoenixs.kafka.core.KafkaMessageSender;
import org.darkphoenixs.kafka.core.ZookeeperHosts;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

public class KafkaMessageSenderPoolTest {

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

		final KafkaMessageSenderPool<byte[], byte[]> pool = new KafkaMessageSenderPool<byte[], byte[]>();

		Assert.assertNotNull(pool.getThreadFactory());
		pool.setThreadFactory(new KafkaPoolThreadFactory());

		Assert.assertNotNull(pool.getProps());
		pool.setProps(new Properties());

		Assert.assertEquals(0, pool.getPoolSize());
		pool.setPoolSize(10);

		Assert.assertNull(pool.getClientId());
		pool.setClientId("test");

		Assert.assertNull(pool.getBrokerStr());
		pool.setBrokerStr("");

		Assert.assertNull(pool.getConfig());
		pool.setConfig(new DefaultResourceLoader()
				.getResource("kafka/producer1.properties"));
		pool.setConfig(new DefaultResourceLoader()
				.getResource("kafka/producer.properties"));

		Thread thread = new Thread(new Runnable() {

			@Override
			public void run() {
				pool.setZkhosts(new ZookeeperHosts(props
						.getProperty(KafkaConstants.ZOOKEEPER_LIST),
						"QUEUE.TEST"));
			}
		});

		thread.setDaemon(true);

		thread.start();

		Assert.assertNotNull(pool.getSender(-1));

		Assert.assertNotNull(pool.getSender(10000));

		pool.init();

		KafkaMessageSender<byte[], byte[]> sender = pool.getSender(0);

		pool.returnSender(sender);
		pool.returnSender(sender);

		pool.destroy();
	}

	@Test
	public void test1() throws Exception {

		final KafkaMessageSenderPool<byte[], byte[]> pool = new KafkaMessageSenderPool<byte[], byte[]>();

		Thread thread = new Thread(new Runnable() {

			@Override
			public void run() {
				pool.init();
			}
		});

		thread.setDaemon(true);

		thread.start();

		pool.destroy();
	}
}
