/**
 * <p>Title: MessageProducer.java</p>
 * <p>Description: MessageProducer</p>
 * <p>Package: org.darkphoenixs.kafka.producer</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.kafka.producer;

import org.darkphoenixs.mq.exception.MQException;

/**
 * <p>Title: MessageProducer</p>
 * <p>Description: 消息生产者</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see AbstractProducer
 * @version 1.0
 */
public class MessageProducer<T> extends AbstractProducer<T> {

	@Override
	protected T doSend(T message) throws MQException {

		return message;
	}
}
