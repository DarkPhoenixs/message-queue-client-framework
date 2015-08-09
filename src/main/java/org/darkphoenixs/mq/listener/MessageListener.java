/**
 * <p>Title: MessageListener.java</p>
 * <p>Description: MessageListener</p>
 * <p>Package: org.darkphoenixs.mq.listener</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.mq.listener;

import org.darkphoenixs.mq.exception.MQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: MessageListener</p>
 * <p>Description: 消息监听器接口</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public interface MessageListener<T> {

	/** 日志对象 */
	public static final Logger logger = LoggerFactory.getLogger(MessageListener.class);
	
	/**
	 * <p>Title: onMessage</p>
	 * <p>Description: 监听方法</p>
	 *
	 * @param message 消息
	 * @throws MQException MQ异常
	 */
	public abstract void onMessage(final T message) throws MQException;
}
