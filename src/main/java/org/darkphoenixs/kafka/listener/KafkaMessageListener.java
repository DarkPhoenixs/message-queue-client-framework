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
package org.darkphoenixs.kafka.listener;

import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.listener.MQMessageListener;

import java.util.Map;

/**
 * <p>KafkaMessageListener</p>
 * <p>Kafka消息监听器基类</p>
 *
 * @param <K> the type parameter
 * @param <V> the type parameter
 * @author Victor.Zxy
 * @version 1.3.0
 * @see MQMessageListener
 * @since 2016年7月21日
 */
public abstract class KafkaMessageListener<K, V> implements MQMessageListener<V> {

    /**
     * <p>onMessage</p>
     * <p>监听方法</p>
     *
     * @param key 标识
     * @param val 消息
     * @throws MQException the mq exception
     */
    public void onMessage(final K key, final V val) throws MQException {

        onMessage(val);
    }

    @Override
    public void onMessage(V message) throws MQException {
        // For compatible without Key.
    }


    /**
     * <p>onMessage</p>
     * <p>监听方法</p>
     *
     * @param messages 消息键值对
     * @throws MQException the mq exception
     * @since 1.4.3
     */
    public void onMessage(Map<K, V> messages) throws MQException {
        // For batch consumer messages.
    }
}
