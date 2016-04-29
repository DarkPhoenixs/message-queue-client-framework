package org.darkphoenixs.mq;

import org.darkphoenixs.kafka.codec.MessageEncoderImpl;
import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.core.KafkaMessageTemplate;
import org.darkphoenixs.kafka.pool.KafkaMessageSenderPool;
import org.darkphoenixs.kafka.producer.MessageProducer;
import org.darkphoenixs.mq.codec.MessageEncoder;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;

public class SenderTest {

	@Test
	public void test() throws Exception {
		
		MessageEncoder<MessageBeanImpl> messageEncoder = new MessageEncoderImpl();
		
		KafkaDestination kafkaDestination = new KafkaDestination("QUEUE.TEST");

		KafkaMessageSenderPool<byte[], byte[]> pool = new KafkaMessageSenderPool<byte[], byte[]>();
		
		pool.setConfig(new DefaultResourceLoader().getResource("kafka/producer.properties"));
		
		pool.setPoolSize(10);
		
		KafkaMessageTemplate<MessageBeanImpl> messageTemplate = new KafkaMessageTemplate<MessageBeanImpl>();
		
		messageTemplate.setEncoder(messageEncoder);
		
		messageTemplate.setMessageSenderPool(pool);
		
		MessageProducer<MessageBeanImpl> messageProducer = new MessageProducer<MessageBeanImpl>();
		
		messageProducer.setMessageTemplate(messageTemplate);
		
		messageProducer.setDestination(kafkaDestination);
		
	}
}
