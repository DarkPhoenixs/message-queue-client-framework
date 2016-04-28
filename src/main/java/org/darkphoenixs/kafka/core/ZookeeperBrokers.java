/**
 * <p>Title: ZookeeperBrokers.java</p>
 * <p>Description: ZookeeperBrokers</p>
 * <p>Package: org.darkphoenixs.kafka.core</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.kafka.core;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * <p>Title: ZookeeperBrokers</p>
 * <p>Description: ZookeeperBrokers</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public class ZookeeperBrokers {

	public static final Logger logger = LoggerFactory
			.getLogger(ZookeeperBrokers.class);

	private CuratorFramework _curator;
	private String _zkPath;
	private String _topic;

	
	public ZookeeperBrokers(ZookeeperHosts zkHosts) {
		_zkPath = zkHosts.getBrokerZkPath();
		_topic = zkHosts.getTopic();
		_curator = CuratorFrameworkFactory.newClient(zkHosts.getBrokerZkStr(), new RetryNTimes(
				Integer.MAX_VALUE, KafkaConstants.INTERVAL_IN_MS));
		_curator.start();
	}
	
	public ZookeeperBrokers(String zkStr, String zkPath, String topic) {
		_zkPath = zkPath;
		_topic = topic;
		_curator = CuratorFrameworkFactory.newClient(zkStr, new RetryNTimes(
				Integer.MAX_VALUE, KafkaConstants.INTERVAL_IN_MS));
		_curator.start();
	}

	/**
	 * Get all partitions with their current leaders
	 */
	public String getBrokerInfo() {
		String brokerStr = "";
		try {
			int numPartitionsForTopic = getNumPartitions();
			String brokerInfoPath = brokerPath();
			for (int partition = 0; partition < numPartitionsForTopic; partition++) {
				int leader = getLeaderFor(partition);
				String path = brokerInfoPath + "/" + leader;
				try {
					byte[] hostPortData = _curator.getData().forPath(path);
					brokerStr = brokerStr + getBrokerHost(hostPortData);
					if (partition != numPartitionsForTopic - 1)
						brokerStr += ",";
				} catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
					logger.error("Node {} does not exist ", path);
				}
			}
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		logger.info("Read partition info from zookeeper: " + brokerStr);
		return brokerStr;
	}

	/**
	 * Get partitions number
	 */
	public int getNumPartitions() {
		try {
			String topicBrokersPath = partitionPath();
			List<String> children = _curator.getChildren().forPath(
					topicBrokersPath);
			return children.size();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * get /brokers/topics/distributedTopic/partitions/1/state {
	 * "controller_epoch":4, "isr":[ 1, 0 ], "leader":1, "leader_epoch":1,
	 * "version":1 }
	 * 
	 * @param partition
	 * @return leader partition number
	 */
	public int getLeaderFor(long partition) {
		try {
			String topicBrokersPath = partitionPath();
			byte[] hostPortData = _curator.getData().forPath(
					topicBrokersPath + "/" + partition + "/state");
			JSONObject json = JSON.parseObject(new String(hostPortData, "UTF-8"));
			Integer leader = json.getInteger("leader");
			return leader;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 
	 * [zk: localhost:2181(CONNECTED) 56] get /brokers/ids/0 {
	 * "host":"localhost", "jmx_port":9999, "port":9092, "version":1 }
	 * 
	 * @param contents
	 * @return broker host
	 */
	public String getBrokerHost(byte[] contents) {
		try {
			JSONObject json = JSON.parseObject(new String(contents, "UTF-8"));
			String host = json.getString("host");
			Integer port = json.getInteger("port");
			return host + ":" + port;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Get partition path
	 */
	public String partitionPath() {
		return _zkPath + "/topics/" + _topic + "/partitions";
	}

	/**
	 * Get broker path
	 */
	public String brokerPath() {
		return _zkPath + "/ids";
	}
	
	/**
	 * Close curator
	 */
	public void close() {
		_curator.close();
	}
}
