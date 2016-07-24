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
	String AUTO_COMMIT_ENABLE ="auto.commit.enable";

	int DEFAULT_REFRESH_FRE_SEC = 60;
	int INIT_TIMEOUT_MIN = 2; // 2min
	int INIT_TIMEOUT_MS = 5000; // 5000ms

	int ZOOKEEPER_SESSION_TIMEOUT = 100; // in ms
	int INTERVAL_IN_MS = 100;
	int WAIT_TIME_MS = 2000;

	int BUFFER_SIZE = 64 * 1024;
	int FETCH_SIZE = 100000;
	int SO_TIMEOUT = 100000;
}
