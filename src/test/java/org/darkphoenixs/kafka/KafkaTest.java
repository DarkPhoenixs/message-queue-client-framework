package org.darkphoenixs.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.admin.TopicCommand;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaTest {
	
	private int brokerId = 0;
	private String topic = "QUEUE.TEST";
	private String zkConnect;
	private EmbeddedZookeeper zkServer;
	private ZkClient zkClient;
	private KafkaServer kafkaServer;
	private int port;
	private Properties kafkaProps;

	@Before
	public void before() {
		zkConnect = TestZKUtils.zookeeperConnect();
		zkServer = new EmbeddedZookeeper(zkConnect);
		zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
		
		// setup Broker
		port = TestUtils.choosePort();
		kafkaProps = TestUtils.createBrokerConfig(brokerId, port, true);

		KafkaConfig config = new KafkaConfig(kafkaProps);
		Time mock = new MockTime();
		kafkaServer = TestUtils.createServer(config, mock);

		// create topic
		TopicCommand.TopicCommandOptions options = new TopicCommand.TopicCommandOptions(new String[] { "--create", "--topic", topic, "--replication-factor",
				"1", "--partitions", "1" });

		TopicCommand.createTopic(zkClient, options);

		List<KafkaServer> servers = new ArrayList<KafkaServer>();
		servers.add(kafkaServer);
		TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);
	}

	@After
	public void after() {
		kafkaServer.shutdown();
        zkClient.close();
		zkServer.shutdown();
	}
	
	

	@Test
	public void testOnlySend() {
		
		// setup producer
		Properties properties = TestUtils.getProducerConfig("localhost:" + port);
		properties.put("serializer.class", StringEncoder.class.getCanonicalName());

		ProducerConfig pConfig = new ProducerConfig(properties);
		Producer<Integer, String> producer = new Producer<>(pConfig);

		// send message
		KeyedMessage<Integer, String> data = new KeyedMessage<>(topic, "test-message");

		List<KeyedMessage<Integer, String>> messages = new ArrayList<KeyedMessage<Integer, String>>();
		messages.add(data);
		// producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
		producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));

		// cleanup
		producer.close();

	}

	@Test
	public void testSendAndReceive() {
		Properties consumerProps = TestUtils.createConsumerProperties(zkServer.connectString(), "group_1", "consumer_id", 1000);
		consumerProps.putAll(kafkaProps);
		ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		// now launch all the threads
		ExecutorService executor = Executors.newFixedThreadPool(2);

		// now create an object to consume the messages
		int threadNumber = 0;
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new ConsumerThread(stream, threadNumber));
			threadNumber++;
		}
		// setup producer
		Properties properties = TestUtils.getProducerConfig("localhost:" + port);
		properties.put("serializer.class", StringEncoder.class.getCanonicalName());

		ProducerConfig pConfig = new ProducerConfig(properties);
		Producer<Integer, String> producer = new Producer<>(pConfig);

		// send message
		for (int i = 0; i < 10; i++) {
			KeyedMessage<Integer, String> data = new KeyedMessage<>(topic, "test-message-" + i);

			List<KeyedMessage<Integer, String>> messages = new ArrayList<KeyedMessage<Integer, String>>();
			messages.add(data);
			// producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
			producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
		}

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		executor.shutdownNow();
		producer.close();
	}

	static class ConsumerThread extends Thread {
		private KafkaStream<byte[], byte[]> stream;
		private int threadNumber;

		public ConsumerThread(KafkaStream<byte[], byte[]> stream, int threadNumber) {
            this.stream = stream;
            this.threadNumber = threadNumber;

		}

		public void run() {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            int count = 10;
            while (it.hasNext() && count-->0) {
                MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
                logMessage(messageAndMetadata);
            }
		}

		void logMessage(MessageAndMetadata<byte[], byte[]>  messageAndMetadata) {
			System.out.println("=====================================================");
            System.out.println("THREAD: " + threadNumber);
			System.out.println("RECEIVED BY: " + this.getClass().getCanonicalName());
			System.out.println("TOPIC: " + messageAndMetadata.topic());
			System.out.println("OFFSET: " + messageAndMetadata.offset());
			System.out.println("PARTITION: " + messageAndMetadata.partition());
			System.out.println("KEY: " + messageAndMetadata.key());
			System.out.println("MESSAGE: " + (messageAndMetadata.message() == null ? "null" : new String(messageAndMetadata.message())));
		}

	}
}