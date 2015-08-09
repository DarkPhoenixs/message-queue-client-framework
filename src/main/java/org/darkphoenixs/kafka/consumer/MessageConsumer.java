/**
 * <p>Title: MessageConsumer.java</p>
 * <p>Description: MessageConsumer</p>
 * <p>Package: org.darkphoenixs.kafka.consumer</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.kafka.consumer;

import org.darkphoenixs.mq.consumer.AbstractConsumer;
import org.darkphoenixs.mq.exception.MQException;

/**
 * <p>Title: MessageConsumer</p>
 * <p>Description: 消息消费者</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see AbstractConsumer
 * @version 1.0
 */
public class MessageConsumer<T> extends AbstractConsumer<T> {

	@Override
	protected void doReceive(T message) throws MQException {

		System.out.println(message);
	}
}
