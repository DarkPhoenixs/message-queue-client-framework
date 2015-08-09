/**
 * <p>Title: AbstractProducer.java</p>
 * <p>Description: AbstractProducer</p>
 * <p>Package: org.darkphoenixs.kafka.producer</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.kafka.producer;

import org.darkphoenixs.kafka.core.KafkaMessageTemplate;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.producer.Producer;

/**
 * <p>Title: AbstractProducer</p>
 * <p>Description: 生产者抽象类</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see Producer
 * @version 1.0
 */
public abstract class AbstractProducer<T> implements Producer<T>  {

	/** protocolId */
	private String protocolId;
	
	/** messageTemplate */
	private KafkaMessageTemplate<T> messageTemplate;
	
	/** topic */
	private String destination;

	/**
	 * @return the messageTemplate
	 */
	public KafkaMessageTemplate<T> getMessageTemplate() {
		return messageTemplate;
	}

	/**
	 * @param messageTemplate the messageTemplate to set
	 */
	public void setMessageTemplate(KafkaMessageTemplate<T> messageTemplate) {
		this.messageTemplate = messageTemplate;
	}
	
	/**
	 * @return the destination
	 */
	public String getDestination() {
		return destination;
	}

	/**
	 * @param destination the destination to set
	 */
	public void setDestination(String destination) {
		this.destination = destination;
	}

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

	@Override
	public void send(T message) throws MQException {

		try {
			T obj = doSend(message);

			messageTemplate.convertAndSend(destination, obj);

		} catch (Exception e) {

			throw new MQException(e);
		}

		logger.debug("Send Success, ProducerKey : " + this.getProducerKey()
				+ " , Message : " + message);

	}
	
	@Override
	public String getProducerKey() throws MQException {

		return destination;
	}
	
	/**
	 * <p>Title: doSend</p>
	 * <p>Description: 消息发送方法</p>
	 *
	 * @param message 消息
	 * @return 消息
	 * @throws MQException MQ异常
	 */
	protected abstract T doSend(T message) throws MQException;
}
