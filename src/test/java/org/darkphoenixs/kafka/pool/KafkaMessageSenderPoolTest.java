package org.darkphoenixs.kafka.pool;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;

public class KafkaMessageSenderPoolTest {

	@Test
	public void test() throws Exception {
		
		KafkaMessageSenderPool<byte[], byte[]> pool = new KafkaMessageSenderPool<byte[], byte[]>();
		
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
		
		pool.init();
		
		Assert.assertNotNull(pool.getSender(0));
		
		pool.destroy();
	}
}
