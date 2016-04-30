package org.darkphoenixs.kafka.pool;

import java.util.Properties;

import org.darkphoenixs.kafka.core.KafkaMessageSender;
import org.darkphoenixs.kafka.core.ZookeeperHosts;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;

public class KafkaMessageSenderPoolTest {

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
		pool.setConfig(new DefaultResourceLoader().getResource("kafka/producer1.properties"));
		pool.setConfig(new DefaultResourceLoader().getResource("kafka/producer.properties"));
		
		pool.setPoolSize(0);

		Thread thread = new Thread(new Runnable() {

			@Override
			public void run() {
				pool.setZkhosts(new ZookeeperHosts("10.0.63.10:2181", "QUEUE.TEST"));
			}
		});
		
		thread.setDaemon(true);

		thread.start();
		
		Assert.assertNotNull(pool.getSender(-1));

		Assert.assertNotNull(pool.getSender(10000));

		pool.init();
		
		KafkaMessageSender<byte[], byte[]> sender= pool.getSender(0);
		
		pool.returnSender(sender);
		pool.returnSender(sender);

		pool.destroy();
	}
}
