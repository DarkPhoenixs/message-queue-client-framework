/**
 * <p>Title: Producer.java</p>
 * <p>Description: Producer</p>
 * <p>Package: org.darkphoenixs.mq.producer</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.mq.producer;

import org.darkphoenixs.mq.exception.MQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: Producer</p>
 * <p>Description: 生产者接口</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public interface Producer<T> {

	/** 日志对象 */
	public static final Logger logger = LoggerFactory.getLogger(Producer.class);

	/**
	 * <p>Title: send</p>
	 * <p>Description: 发送消息</p>
	 *
	 * @param message 消息
	 * @throws MQException MQ异常
	 */
	public abstract void send(T message) throws MQException;

	/**
	 * <p>Title: getProducerKey</p>
	 * <p>Description: 生产者标识</p>
	 *
	 * @return 生产者标识
	 * @throws MQException MQ异常
	 */
	public abstract String getProducerKey() throws MQException;

}
