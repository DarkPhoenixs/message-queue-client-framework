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

import org.darkphoenixs.mq.codec.MQMessageDecoder;
import org.darkphoenixs.mq.exception.MQException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>KafkaMessageDecoder</p>
 * <p>Kafka消息解码器基类</p>
 *
 * @author Victor.Zxy
 * @version 1.3.0
 * @see MQMessageDecoder
 * @since 2016年7月21日
 */
public abstract class KafkaMessageDecoder<K, V> implements MQMessageDecoder<V> {

    @Override
    public V decode(byte[] bytes) throws MQException {

        return decodeVal(bytes);
    }

    @Override
    public List<V> batchDecode(List<byte[]> bytes) throws MQException {

        List<V> list = new ArrayList<V>();

        for (byte[] bs : bytes)

            list.add(this.decode(bs));

        return list;
    }

    /**
     * <p>decodeKey</p>
     * <p>标识反序列化</p>
     *
     * @param bytes 标识
     * @return 反序列化标识
     * @throws MQException
     */
    public abstract K decodeKey(byte[] bytes) throws MQException;

    /**
     * <p>decodeVal</p>
     * <p>消息反序列化</p>
     *
     * @param bytes 消息
     * @return 反序列化消息
     * @throws MQException
     */
    public abstract V decodeVal(byte[] bytes) throws MQException;

    /**
     * <p>batchDecode</p>
     * <p>批量反序列化</p>
     *
     * @param bytes 标识&消息列表
     * @return 反序列化标识&消息列表
     * @throws MQException
     */
    public abstract Map<K, V> batchDecode(Map<byte[], byte[]> bytes) throws MQException;
}
