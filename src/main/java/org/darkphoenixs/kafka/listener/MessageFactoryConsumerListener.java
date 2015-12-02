/**
 * <p>Title: MessageFactoryConsumerListener.java</p>
 * <p>Description: MessageFactoryConsumerListener</p>
 * <p>Package: org.darkphoenixs.kafka.listener</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.kafka.listener;

import org.darkphoenixs.mq.consumer.Consumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.ConsumerFactory;
import org.darkphoenixs.mq.listener.MessageListener;
import org.darkphoenixs.mq.message.AbstractMessageBean;

/**
 * <p>Title: MessageFactoryConsumerListener</p>
 * <p>Description: 消费者工厂监听器</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see MessageListener
 * @version 1.0
 */
public class MessageFactoryConsumerListener implements MessageListener<AbstractMessageBean> {

	/** consumerFactory */
	private ConsumerFactory consumerFactory;

	/**
	 * @return the consumerFactory
	 */
	public ConsumerFactory getConsumerFactory() {
		return consumerFactory;
	}

	/**
	 * @param consumerFactory
	 *            the consumerFactory to set
	 */
	public void setConsumerFactory(ConsumerFactory consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	@Override
	public void onMessage(AbstractMessageBean message) throws MQException {

		if (consumerFactory == null)	
			throw new MQException("ConsumerFactory is null !");

		if (message == null)	
			throw new MQException("Message is null !");
		
		String messageType = message.getMessageType();
		
		if (messageType == null)	
			throw new MQException("Message Type is null !");
		
		Consumer<AbstractMessageBean> consumer = consumerFactory.getConsumer(messageType);
		
		if (consumer == null)
			throw new MQException("Consumer is null !");
		
		consumer.receive(message);
	}
}
