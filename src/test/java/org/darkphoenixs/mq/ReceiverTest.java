package org.darkphoenixs.mq;

import org.darkphoenixs.kafka.codec.MessageDecoderImpl;
import org.darkphoenixs.kafka.consumer.MessageConsumer;
import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.core.KafkaMessageAdapter;
import org.darkphoenixs.kafka.listener.MessageConsumerListener;
import org.darkphoenixs.kafka.pool.KafkaMessageReceiverPool;
import org.darkphoenixs.mq.codec.MessageDecoder;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;

public class ReceiverTest {

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
				
		KafkaMessageReceiverPool<byte[], byte[]> pool = new KafkaMessageReceiverPool<byte[], byte[]>();
		
		pool.setMessageAdapter(messageAdapter);
		
		pool.setConfig(new DefaultResourceLoader().getResource("kafka/consumer.properties"));
		
		pool.setPoolSize(10);
		
		pool.init();
	}
}
