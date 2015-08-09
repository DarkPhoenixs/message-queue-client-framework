/**
 * <p>Title: KafkaConstants.java</p>
 * <p>Description: KafkaConstants</p>
 * <p>Package: org.darkphoenixs.kafka.core</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.kafka.core;

/**
 * <p>Title: KafkaConstants</p>
 * <p>Description: Kafka常量接口</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public interface KafkaConstants {

	String DEFAULT_ZK_ROOT = "/brokers";
	String BROKER_LIST = "metadata.broker.list";
	String ZOOKEEPER_LIST = "zookeeper.connect";
	String PRODUCER_TYPE = "producer.type";
	String CLIENT_ID = "client.id";
	String SERIALIZER_CLASS = "serializer.class";
	String KEY_SERIALIZER_CLASS = "key.serializer.class";

	int DEFAULT_REFRESH_FRE_SEC = 60;
	int INIT_TIMEOUT_MIN = 2; // 2min

	int ZOOKEEPER_SESSION_TIMEOUT = 100; // in ms
	int INTERVAL_IN_MS = 100;
	int WAIT_TIME_MS = 2000;

	int BUFFER_SIZE = 64 * 1024;
	int FETCH_SIZE = 100000;
	int SO_TIMEOUT = 100000;
}
