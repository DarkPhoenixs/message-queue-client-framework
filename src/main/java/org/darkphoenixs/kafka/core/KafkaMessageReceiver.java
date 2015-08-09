/**
 * <p>Title: KafkaMessageReceiver.java</p>
 * <p>Description: KafkaMessageReceiver</p>
 * <p>Package: org.darkphoenixs.kafka.core</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.kafka.core;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: KafkaMessageReceiver</p>
 * <p>Description: Kafka消息接收接口</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public interface KafkaMessageReceiver<K, V> {

	/** logger */
	Logger logger = LoggerFactory.getLogger(KafkaMessageReceiver.class);
	
	/**
	 * <p>Title: receive</p>
	 * <p>Description: Receive the msg from Kafka</p>
	 *
	 * @param topic Topic name
	 * @param partition Partition number
	 * @param beginOffset Begin the offset index
	 * @param readOffset Number of read messages
	 * @return message
	 */
	List<V> receive(String topic, int partition, long beginOffset, long readOffset);
	
	/**
	 * <p>Title: receiveWithKey</p>
	 * <p>Description: Receive the msg from Kafka</p>
	 *
	 * @param topic Topic name
	 * @param partition Partition number
	 * @param beginOffset Begin the offset index
	 * @param readOffset Number of read messages
	 * @return message
	 */
	Map<K, V> receiveWithKey(String topic, int partition, long beginOffset, long readOffset);
	
	/**
	 * <p>Title: getLatestOffset</p>
	 * <p>Description: Get latest offset number</p>
	 *
	 * @param topic Topic name
	 * @param partition Partition number
	 * @return the latest offset
	 */
	long getLatestOffset(String topic, int partition);
	
	/**
	 * <p>Title: getEarliestOffset</p>
	 * <p>Description: Get earliest offset number</p>
	 *
	 * @param topic Topic name
	 * @param partition Partition number
	 * @return the earliest offset
	 */
	long getEarliestOffset(String topic, int partition);

	/**
	 * <p>Title: close</p>
	 * <p>Description: Close this receiver</p>
	 *
	 */
	void close();
}
