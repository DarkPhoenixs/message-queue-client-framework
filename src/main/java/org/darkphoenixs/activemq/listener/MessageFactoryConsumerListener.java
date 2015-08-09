/**
 * <p>Title: MessageFactoryConsumerListener.java</p>
 * <p>Description: MessageFactoryConsumerListener</p>
 * <p>Package: org.darkphoenixs.activemq.listener</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.activemq.listener;

import java.util.concurrent.ExecutorService;

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

	/** threadPool */
	private ExecutorService threadPool;

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

	/**
	 * @return the threadPool
	 */
	public ExecutorService getThreadPool() {
		return threadPool;
	}

	/**
	 * @param threadPool
	 *            the threadPool to set
	 */
	public void setThreadPool(ExecutorService threadPool) {
		this.threadPool = threadPool;
	}

	@Override
	public void onMessage(final T message) throws MQException {

		if (consumerFactory != null) {

			if (message instanceof AbstractMessageBean) {

				final AbstractMessageBean messageBean = (AbstractMessageBean) message;

				final Consumer<AbstractMessageBean> consumer = consumerFactory
						.getConsumer(messageBean.getMessageType());

				if (consumer != null) {

					if (threadPool != null) {

						threadPool.execute(new Runnable() {

							@Override
							public void run() {

								try {
									consumer.receive(messageBean);
								} catch (MQException e) {
									logger.error(e.getMessage());
								}
							}
						});
					} else
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
