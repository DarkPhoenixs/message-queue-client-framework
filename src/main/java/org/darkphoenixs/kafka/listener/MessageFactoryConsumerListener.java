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
public class MessageFactoryConsumerListener<T> implements MessageListener<T> {

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
	public void onMessage(T message) throws MQException {

		if (consumerFactory != null) {

			if (message instanceof AbstractMessageBean) {

				final AbstractMessageBean messageBean = (AbstractMessageBean) message;

				final Consumer<AbstractMessageBean> consumer = consumerFactory
						.getConsumer(messageBean.getMessageType());

				if (consumer != null) {

					consumer.receive(messageBean);
				} else
					throw new MQException("Consumer is null !");
			} else
				throw new MQException(
						"Message is not instanceof AbstractMessageBean ! Message is "
								+ message);
		} else
			throw new MQException("ConsumerFactory is null !");
	}

}
