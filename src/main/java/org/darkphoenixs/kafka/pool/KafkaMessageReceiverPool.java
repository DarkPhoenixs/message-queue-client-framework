/**
 * <p>Title: KafkaMessageReceiverPool.java</p>
 * <p>Description: KafkaMessageReceiverPool</p>
 * <p>Package: org.darkphoenixs.kafka.pool</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.kafka.pool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;

import org.darkphoenixs.kafka.core.KafkaConstants;
import org.darkphoenixs.kafka.core.KafkaMessageAdapter;
import org.darkphoenixs.kafka.core.KafkaMessageReceiver;
import org.darkphoenixs.kafka.core.KafkaMessageReceiverImpl;
import org.darkphoenixs.kafka.core.ReflectionTool;
import org.darkphoenixs.kafka.core.ZookeeperBrokers;
import org.darkphoenixs.kafka.core.ZookeeperHosts;
import org.darkphoenixs.mq.exception.MQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

/**
 * <p>Title: KafkaMessageReceiverPool</p>
 * <p>Description: Kafka消息发送线程池</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public class KafkaMessageReceiverPool<K, V> {

	private static final Logger logger = LoggerFactory
			.getLogger(KafkaMessageReceiverPool.class);

	/** consumer */
	protected ConsumerConnector consumer;
	/** pool */
	protected ExecutorService pool;
	/** props */
	protected Properties props = new Properties();

	/** messageAdapter */
	private KafkaMessageAdapter<?> messageAdapter;

	/** poolSize */
	private int poolSize;
	/** config */
	private Resource config;

	/** keyDecoder */
	private Class<?> keyDecoderClass = DefaultDecoder.class;
	/** valDecoder */
	private Class<?> valDecoderClass = DefaultDecoder.class;

	/**
	 * @param poolSize
	 *            the poolSize to set
	 */
	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
		this.pool = Executors.newFixedThreadPool(poolSize);
	}

	/**
	 * @param zookeeperStr
	 *            the zookeeperStr to set
	 */
	public void setZookeeperStr(String zookeeperStr) {
		props.put(KafkaConstants.ZOOKEEPER_LIST, zookeeperStr);
	}

	/**
	 * @param clientId
	 *            the clientId to set
	 */
	public void setClientId(String clientId) {
		props.put(KafkaConstants.CLIENT_ID, clientId);
	}

	/**
	 * @param config
	 *            the config to set
	 */
	public void setConfig(Resource config) {
		this.config = config;
		try {
			PropertiesLoaderUtils.fillProperties(props, this.config);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * @return the clientId
	 */
	public String getClientId() {
		return props.getProperty(KafkaConstants.CLIENT_ID);
	}

	/**
	 * @return the zookeeperStr
	 */
	public String getZookeeperStr() {
		return props.getProperty(KafkaConstants.ZOOKEEPER_LIST);
	}

	/**
	 * @return the props
	 */
	public Properties getProps() {
		return props;
	}

	/**
	 * @param props
	 *            the props to set
	 */
	public void setProps(Properties props) {
		this.props = props;
	}

	/**
	 * @return the poolSize
	 */
	public int getPoolSize() {
		return poolSize;
	}

	/**
	 * @return the config
	 */
	public Resource getConfig() {
		return config;
	}

	/**
	 * @return the keyDecoderClass
	 */
	public Class<?> getKeyDecoderClass() {
		return keyDecoderClass;
	}

	/**
	 * @param keyDecoderClass
	 *            the keyDecoderClass to set
	 */
	public void setKeyDecoderClass(Class<?> keyDecoderClass) {
		this.keyDecoderClass = keyDecoderClass;
	}

	/**
	 * @return the valDecoderClass
	 */
	public Class<?> getValDecoderClass() {
		return valDecoderClass;
	}

	/**
	 * @param valDecoderClass
	 *            the valDecoder to set
	 */
	public void setValDecoderClass(Class<?> valDecoderClass) {
		this.valDecoderClass = valDecoderClass;
	}

	/**
	 * @return the messageAdapter
	 */
	public KafkaMessageAdapter<?> getMessageAdapter() {
		return messageAdapter;
	}

	/**
	 * @param messageAdapter
	 *            the messageAdapter to set
	 */
	public void setMessageAdapter(KafkaMessageAdapter<?> messageAdapter) {
		this.messageAdapter = messageAdapter;
	}

	/**
	 * Get broker address
	 * 
	 * @param topic
	 *            topic name
	 * @return broker address
	 */
	public String getBrokerStr(String topic) {

		ZookeeperHosts zkHosts = new ZookeeperHosts(getZookeeperStr(), topic);
		ZookeeperBrokers brokers = new ZookeeperBrokers(zkHosts);
		String brokerStr = brokers.getBrokerInfo();
		brokers.close();
		return brokerStr;
	}

	/**
	 * Get partition number
	 * 
	 * @param topic
	 *            topic name
	 * @return partition number
	 */
	public int getPartitionNum(String topic) {

		ZookeeperHosts zkHosts = new ZookeeperHosts(getZookeeperStr(), topic);
		ZookeeperBrokers brokers = new ZookeeperBrokers(zkHosts);
		int partitionNum = brokers.getNumPartitions();
		brokers.close();
		return partitionNum;
	}

	/**
	 * Get a receiver from the pool (just only create a new receiver).
	 * 
	 * @return a receiver instance
	 */
	public KafkaMessageReceiver<K, V> getReceiver() {

		KafkaMessageReceiver<K, V> receiver = new KafkaMessageReceiverImpl<>(
				props, this);

		return receiver;
	}

	/**
	 * Init the pool.
	 */
	public synchronized void init() {

		String topic = messageAdapter.getDestination();

		int defaultSize = getPartitionNum(topic);

		if (poolSize == 0 || poolSize > defaultSize)

			setPoolSize(defaultSize);

		logger.info("Message receiver pool initializing. poolSize : "
				+ poolSize + " config : " + props.toString());

		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(new ConsumerConfig(props));

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

		topicCountMap.put(topic, new Integer(poolSize));

		VerifiableProperties verProps = new VerifiableProperties(props);

		@SuppressWarnings("unchecked")
		Decoder<K> keyDecoder = (Decoder<K>) ReflectionTool.newInstance(
				keyDecoderClass, verProps);

		@SuppressWarnings("unchecked")
		Decoder<V> valDecoder = (Decoder<V>) ReflectionTool.newInstance(
				valDecoderClass, verProps);

		Map<String, List<KafkaStream<K, V>>> consumerMap = consumer
				.createMessageStreams(topicCountMap, keyDecoder, valDecoder);

		List<KafkaStream<K, V>> streams = consumerMap.get(topic);

		int threadNumber = 0;

		for (final KafkaStream<K, V> stream : streams) {

			pool.submit(new ReceiverThread(stream, messageAdapter, threadNumber));

			threadNumber++;
		}
	}

	/**
	 * Receiver thread to receive message.
	 */
	class ReceiverThread implements Runnable {

		private KafkaStream<K, V> stream;

		private KafkaMessageAdapter<?> adapter;

		private int threadNumber;

		public ReceiverThread(KafkaStream<K, V> stream,
				KafkaMessageAdapter<?> adapter, int threadNumber) {

			this.stream = stream;
			this.threadNumber = threadNumber;
			this.adapter = adapter;
		}

		@Override
		public void run() {

			logger.info("ReceiverThread-" + threadNumber + " clientId: "
					+ stream.clientId() + " start.");

			ConsumerIterator<K, V> it = stream.iterator();

			while (it.hasNext()) {

				MessageAndMetadata<K, V> messageAndMetadata = it.next();

				K key = messageAndMetadata.key();

				V value = messageAndMetadata.message();

				int partition = messageAndMetadata.partition();

				try {
					this.adapter.messageAdapter(key, value);
				} catch (MQException e) {
					logger.error("ReceiverThread-" + threadNumber
							+ " partition: " + partition + " Exception: "
							+ e.getMessage());
				}
			}

			logger.info("ReceiverThread-" + threadNumber + " clientId: "
					+ stream.clientId() + " end.");
		}

	}

	/**
	 * Destroy the pool.
	 */
	public synchronized void destroy() {

		logger.info("Message receiver pool closing.");

		if (consumer != null)
			consumer.shutdown();

		if (pool != null) {
			pool.shutdown();

			try {
				if (!pool.awaitTermination(KafkaConstants.INIT_TIMEOUT_MIN,
						TimeUnit.MINUTES)) {
					logger.warn("Timed out waiting for consumer threads to shut down, exiting uncleanly");
				}
			} catch (InterruptedException e) {
				logger.error("Interrupted during shutdown, exiting uncleanly");
			}
		}
	}

}
