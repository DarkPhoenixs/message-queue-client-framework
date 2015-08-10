/**
 * <p>Title: MessageProducerFactory.java</p>
 * <p>Description: MessageProducerFactory</p>
 * <p>Package: org.darkphoenixs.mq.common</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.mq.common;

import java.util.concurrent.ConcurrentHashMap;

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

	/** producerCache */
	private ConcurrentHashMap<String, Producer<?>> producerCache = new ConcurrentHashMap<>();

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
	public <T> void addProducer(Producer<T> producer) throws MQException {

		producerCache.put(producer.getProducerKey(), producer);

		logger.debug("Add Producer : " + producer.getProducerKey());

	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Producer<T> getProducer(String producerKey) throws MQException {

		if (producerCache.containsKey(producerKey)) {

			logger.debug("Get Producer : " + producerKey);

			return (Producer<T>) producerCache.get(producerKey);

		} else {

			logger.warn("Unknown ProducerKey : " + producerKey);

			return null;
		}
	}

	@Override
	public void init() throws MQException {

		if (producers != null)

			for (int i = 0; i < producers.length; i++)

				producerCache.put(producers[i].getProducerKey(), producers[i]);

	}

	@Override
	public void destroy() throws MQException {

		if (producers != null)
			producers = null;

		if (instance != null)
			instance = null;

		if (producerCache != null)
			producerCache.clear();

		logger.debug("Destroyed!");
	}
}
