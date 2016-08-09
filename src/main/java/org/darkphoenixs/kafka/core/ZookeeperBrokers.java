/*
 * Copyright 2015-2016 Dark Phoenixs (Open-Source Organization).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.darkphoenixs.kafka.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * <p>Title: ZookeeperBrokers</p>
 * <p>Description: ZookeeperBrokers</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @since 2015-06-01
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
        } catch (Exception e) {
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
