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
package org.darkphoenixs.kafka.codec;

import org.darkphoenixs.mq.codec.MQMessageEncoder;
import org.darkphoenixs.mq.exception.MQException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>KafkaMessageEncoder</p>
 * <p>Kafka消息编码器基类</p>
 *
 * @author Victor.Zxy
 * @version 1.3.0
 * @see MQMessageEncoder
 * @since 2016年7月21日
 */
public abstract class KafkaMessageEncoder<K, V> implements MQMessageEncoder<V> {

    @Override
    public byte[] encode(V message) throws MQException {

        return encodeVal(message);
    }

    @Override
    public List<byte[]> batchEncode(List<V> message) throws MQException {

        List<byte[]> list = new ArrayList<byte[]>();

        for (V v : message)

            list.add(this.encode(v));

        return list;
    }

    /**
     * <p>encodeKey</p>
     * <p>标识序列化</p>
     *
     * @param key 标识
     * @return 序列化标识
     * @throws MQException
     */
    public abstract byte[] encodeKey(K key) throws MQException;

    /**
     * <p>encodeVal</p>
     * <p>消息序列化</p>
     *
     * @param val 消息
     * @return 序列化消息
     * @throws MQException
     */
    public abstract byte[] encodeVal(V val) throws MQException;

    /**
     * <p>batchEncode</p>
     * <p>批量序列化</p>
     *
     * @param messages 标识&消息列表
     * @return 序列化标识&消息列表
     * @throws MQException
     */
    public abstract Map<byte[], byte[]> batchEncode(Map<K, V> messages) throws MQException;
}
