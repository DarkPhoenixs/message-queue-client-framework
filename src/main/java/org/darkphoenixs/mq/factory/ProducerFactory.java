/**
 * <p>Title: ProducerFactory.java</p>
 * <p>Description: ProducerFactory</p>
 * <p>Package: org.darkphoenixs.mq.factory</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.mq.factory;

import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: ProducerFactory</p>
 * <p>Description: 生产者工厂接口</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public interface ProducerFactory {

	/** 日志对象 */
	public static final Logger logger = LoggerFactory.getLogger(ProducerFactory.class);
	
	/**
	 * <p>Title: addProducer</p>
	 * <p>Description: 增加生产者</p>
	 *
	 * @param producer 生产者
	 * @throws MQException MQ异常
	 */
	public <T> void addProducer(Producer<T> producer) throws MQException;

	/**
	 * <p>Title: getProducer</p>
	 * <p>Description: 获得生产者</p>
	 *
	 * @param producerKey 生产者标识
	 * @return 生产者
	 * @throws MQException MQ异常
	 */
	public <T> Producer<T> getProducer(String producerKey) throws MQException;

	/**
	 * <p>Title: init</p>
	 * <p>Description: 初始化工厂</p>
	 *
	 * @throws MQException MQ异常
	 */
	public void init() throws MQException;
	
	/**
	 * <p>Title: destroy</p>
	 * <p>Description: 销毁工厂</p>
	 * 
	 * @throws MQException MQ异常
	 */
	public void destroy() throws MQException;
}
