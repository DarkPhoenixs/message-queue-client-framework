package org.darkphoenixs.kafka.pool;

import java.util.Properties;

import kafka.serializer.DefaultDecoder;

import org.darkphoenixs.kafka.codec.MessageDecoderImpl;
import org.darkphoenixs.kafka.consumer.MessageConsumer;
import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.core.KafkaMessageAdapter;
import org.darkphoenixs.kafka.listener.MessageConsumerListener;
import org.darkphoenixs.mq.codec.MessageDecoder;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;

public class KafkaMessageReceiverPoolTest {

	@Test
	public void test() throws Exception {
		
		MessageDecoder<MessageBeanImpl> messageDecoder = new MessageDecoderImpl();
		
		KafkaDestination kafkaDestination = new KafkaDestination("QUEUE.TEST");
		
		MessageConsumer<MessageBeanImpl> MessageConsumer = new MessageConsumer<MessageBeanImpl>();
		
		MessageConsumerListener<MessageBeanImpl> messageConsumerListener = new MessageConsumerListener<MessageBeanImpl>();
		
		messageConsumerListener.setConsumer(MessageConsumer);
		
		KafkaMessageAdapter<MessageBeanImpl> messageAdapter= new KafkaMessageAdapter<MessageBeanImpl>();
		
		messageAdapter.setDecoder(messageDecoder);
		
		messageAdapter.setDestination(kafkaDestination);
		
		messageAdapter.setMessageListener(messageConsumerListener);
		
		final KafkaMessageReceiverPool<byte[], byte[]> pool = new KafkaMessageReceiverPool<byte[], byte[]>();
		
		Assert.assertNotNull(pool.getThreadFactory());
		pool.setThreadFactory(new KafkaPoolThreadFactory());
		
		Assert.assertNotNull(pool.getProps());
		pool.setProps(new Properties());
		
		Assert.assertEquals(0, pool.getPoolSize());
		pool.setPoolSize(10);
		
		Assert.assertNull(pool.getZookeeperStr());
		pool.setZookeeperStr("");
		
		Assert.assertNull(pool.getClientId());
		pool.setClientId("test");
		
		Assert.assertTrue(pool.getAutoCommit());
		pool.setAutoCommit(false);
		
		Assert.assertNull(pool.getConfig());
		pool.setConfig(new DefaultResourceLoader().getResource("kafka/producer1.properties"));
		
		Assert.assertSame(DefaultDecoder.class, pool.getKeyDecoderClass());
		pool.setKeyDecoderClass(DefaultDecoder.class);
		
		Assert.assertSame(DefaultDecoder.class, pool.getValDecoderClass());
		pool.setValDecoderClass(DefaultDecoder.class);
		
		Assert.assertNull(pool.getMessageAdapter());
		pool.setMessageAdapter(messageAdapter);
		
		Assert.assertNotNull(pool.getReceiver());
		
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
