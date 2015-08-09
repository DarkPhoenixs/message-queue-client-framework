/**
 * <p>Title: AbstractProducer.java</p>
 * <p>Description: AbstractProducer</p>
 * <p>Package: org.darkphoenixs.activemq.producer</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.activemq.producer;

import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Topic;

import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.producer.Producer;
import org.springframework.jms.core.JmsTemplate;

/**
 * <p>Title: AbstractProducer</p>
 * <p>Description: 生产者抽象类</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see Producer
 * @version 1.0
 */
public abstract class AbstractProducer<T> implements Producer<T> {

	/** protocolId */
	private String protocolId;
	
	/** jmsTemplate */
	private JmsTemplate jmsTemplate;

	/** destination */
	private Destination destination;
	
	/**
	 * @return the protocolId
	 */
	public String getProtocolId() {
		return protocolId;
	}

	/**
	 * @param protocolId the protocolId to set
	 */
	public void setProtocolId(String protocolId) {
		this.protocolId = protocolId;
	}

	/**
	 * @return the jmsTemplate
	 */
	public JmsTemplate getJmsTemplate() {
		return jmsTemplate;
	}

	/**
	 * @param jmsTemplate
	 *            the jmsTemplate to set
	 */
	public void setJmsTemplate(JmsTemplate jmsTemplate) {
		this.jmsTemplate = jmsTemplate;
	}

	/**
	 * @return the destination
	 */
	public Destination getDestination() {
		return destination;
	}

	/**
	 * @param destination
	 *            the destination to set
	 */
	public void setDestination(Destination destination) {
		this.destination = destination;
	}

	@Override
	public void send(T message) throws MQException {

		try {
			Object obj = doSend(message);

			jmsTemplate.convertAndSend(destination, obj);

		} catch (Exception e) {

			throw new MQException(e);
		}

		logger.debug("Send Success, ProducerKey : " + this.getProducerKey()
				+ " , Message : " + message);

	}

	@Override
	public String getProducerKey() throws MQException {

		if (destination instanceof Queue)

			try {
				return ((Queue) destination).getQueueName();

			} catch (Exception e) {

				throw new MQException(e);
			}

		else if (destination instanceof Topic)

			try {
				return ((Topic) destination).getTopicName();

			} catch (Exception e) {

				throw new MQException(e);
			}

		else
			return destination.toString();
	}

	/**
	 * <p>Title: doSend</p>
	 * <p>Description: 消息发送方法</p>
	 *
	 * @param message 消息
	 * @return 消息
	 * @throws MQException MQ异常
	 */
	protected abstract Object doSend(T message) throws MQException;
}
