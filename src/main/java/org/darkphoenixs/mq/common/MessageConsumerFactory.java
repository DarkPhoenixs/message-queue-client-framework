/**
 * <p>Title: MessageConsumerFactory.java</p>
 * <p>Description: MessageConsumerFactory</p>
 * <p>Package: org.darkphoenixs.mq.common</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.mq.common;

import java.util.Arrays;
import java.util.List;

import org.darkphoenixs.mq.consumer.Consumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.ConsumerFactory;

/**
 * <p>Title: MessageConsumerFactory</p>
 * <p>Description: 消息消费者工厂</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see ConsumerFactory
 * @version 1.0
 */

public class MessageConsumerFactory implements ConsumerFactory {

	/** instance */
	private static MessageConsumerFactory instance;

	/** consumers */
	private Consumer<?>[] consumers;

	/**
	 * @return the consumers
	 */
	public Consumer<?>[] getConsumers() {
		return consumers;
	}

	/**
	 * @param consumers
	 *            the consumers to set
	 */
	public void setConsumers(Consumer<?>[] consumers) {
		this.consumers = consumers;
	}

	/**
	 * private construction method
	 */
	private MessageConsumerFactory() {
	}

	/**
	 * get singleton instance method
	 */
	public synchronized static ConsumerFactory getInstance() {

		if (instance == null)
			instance = new MessageConsumerFactory();
		return instance;
	}

	@Override
	public synchronized <T> void addConsumer(Consumer<T> consumer) throws MQException {

		if (consumers == null)

			consumers = new Consumer<?>[0];

		List<Consumer<?>> consumerList = Arrays.asList(consumers);

		consumerList.add(consumer);

		setConsumers((Consumer<?>[]) consumerList.toArray());

		logger.debug("Add Consumer : " + consumer.getConsumerKey());
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized <T> Consumer<T> getConsumer(String consumerKey) throws MQException {

		if (null != consumerKey && !"".equals(consumerKey))

			for (int i = 0; i < consumers.length; i++) {

				if (consumers[i].getConsumerKey().equals(consumerKey)
						|| consumers[i].getConsumerKey().endsWith(consumerKey)
						|| consumerKey.endsWith(consumers[i].getConsumerKey())) {

					logger.debug("Get Consumer : " + consumerKey);

					return (Consumer<T>) consumers[i];
				}
			}

		logger.warn("Unknown consumerKey : " + consumerKey);
		return null;
	}

	@Override
	public synchronized void destroy() {

		if (consumers != null)
			consumers = null;

		if (instance != null)
			instance = null;

		logger.debug("Destroyed!");
	}

}
