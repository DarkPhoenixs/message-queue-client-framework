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
package org.darkphoenixs.mq.codec;

import org.darkphoenixs.mq.exception.MQException;

import java.util.List;

/**
 * <p>Title: MQMessageDecoder</p>
 * <p>Description: 消息解码器接口</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @since 2015-06-01
 */
public interface MQMessageDecoder<T> {

    /**
     * <p>Title: decode</p>
     * <p>Description: 消息反序列化</p>
     *
     * @param bytes 消息
     * @return 反序列化消息
     * @throws MQException
     */
    T decode(byte[] bytes) throws MQException;

    /**
     * <p>Title: batchDecode</p>
     * <p>Description: 批量反序列化</p>
     *
     * @param bytes 消息列表
     * @return 反序列化列表
     * @throws MQException
     */
    List<T> batchDecode(List<byte[]> bytes) throws MQException;

}
