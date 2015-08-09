/**
 * <p>Title: MessageProducerFactory.java</p>
 * <p>Description: MessageProducerFactory</p>
 * <p>Package: org.darkphoenixs.mq.common</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.mq.common;

import java.util.Arrays;
import java.util.List;

import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.ProducerFactory;
import org.darkphoenixs.mq.producer.Producer;

/**
 * <p>Title: MessageProducerFactory</p>
 * <p>Description: 消息生产者工厂</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see ProducerFactory
 * @version 1.0
 */
public class MessageProducerFactory implements ProducerFactory {

	/** instance */
	private static MessageProducerFactory instance;

	/** producers */
	private Producer<?>[] producers;

	/**
	 * @return the producers
	 */
	public Producer<?>[] getProducers() {
		return producers;
	}

	/**
	 * @param producers
	 *            the producers to set
	 */
	public void setProducers(Producer<?>[] producers) {
		this.producers = producers;
	}

	/**
	 * private construction method
	 */
	private MessageProducerFactory() {
	}

	/**
	 * get singleton instance method
	 */
	public synchronized static ProducerFactory getInstance() {

		if (instance == null)
			instance = new MessageProducerFactory();
		return instance;
	}

	@Override
	public synchronized <T> void addProducer(Producer<T> producer) throws MQException {

		if (producers == null)

			producers = new Producer[0];

		List<Producer<?>> producerList = Arrays.asList(producers);

		producerList.add(producer);

		setProducers((Producer<?>[]) producerList.toArray());

		logger.debug("Add Producer : " + producer.getProducerKey());

	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized <T> Producer<T> getProducer(String producerKey) throws MQException {

		if (null != producerKey && !"".equals(producerKey))

			for (int i = 0; i < producers.length; i++) {

				if (producers[i].getProducerKey().equals(producerKey)
						|| producers[i].getProducerKey().endsWith(producerKey)
						|| producerKey.endsWith(producers[i].getProducerKey())) {

					logger.debug("Get Producer : " + producerKey);
					
					return (Producer<T>) producers[i];
				}
			}

		logger.warn("Unknown producerKey : " + producerKey);
		return null;
	}

	@Override
	public synchronized void destroy() {

		if (producers != null)
			producers = null;

		if (instance != null)
			instance = null;

		logger.debug("Destroyed!");
	}

}
