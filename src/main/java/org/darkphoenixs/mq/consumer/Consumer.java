/**
 * <p>Title: Consumer.java</p>
 * <p>Description: Consumer</p>
 * <p>Package: org.darkphoenixs.mq.consumer</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.mq.consumer;

import org.darkphoenixs.mq.exception.MQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: Consumer</p>
 * <p>Description: 消费者接口</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public interface Consumer<T> {

	/** 日志对象 */
	public static final Logger logger = LoggerFactory.getLogger(Consumer.class);
	
	/**
	 * <p>Title: receive</p>
	 * <p>Description: 接收消息</p>
	 *
	 * @param message 消息
	 * @throws MQException MQ异常
	 */
	public abstract void receive(T message) throws MQException;

	/**
	 * <p>Title: getConsumerKey</p>
	 * <p>Description: 消费者标识</p>
	 *
	 * @return 消费者标识
	 * @throws MQException MQ异常
	 */
	public abstract String getConsumerKey() throws MQException;
}
