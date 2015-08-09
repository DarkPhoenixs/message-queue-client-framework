/**
 * <p>Title: MessageConsumerListener.java</p>
 * <p>Description: MessageConsumerListener</p>
 * <p>Package: org.darkphoenixs.kafka.listener</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.kafka.listener;

import org.darkphoenixs.mq.consumer.Consumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.listener.MessageListener;

/**
 * <p>Title: MessageConsumerListener</p>
 * <p>Description: 消费者监听器</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see MessageListener
 * @version 1.0
 */
public class MessageConsumerListener<T> implements MessageListener<T> {

	/** messageConsumer */
	private Consumer<T> consumer;
	
	/**
	 * @return the consumer
	 */
	public Consumer<T> getConsumer() {
		return consumer;
	}

	/**
	 * @param consumer the consumer to set
	 */
	public void setConsumer(Consumer<T> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void onMessage(T message) throws MQException {

		if (consumer != null)
			
			consumer.receive(message);
		else
			throw new MQException("Consumer is null !");

	}
}
