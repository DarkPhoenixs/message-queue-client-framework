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
package org.darkphoenixs.mq.consumer;

import org.darkphoenixs.mq.exception.MQException;

/**
 * <p>Title: MQConsumer</p>
 * <p>Description: 消费者接口</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @since 2015-06-01
 */
public interface MQConsumer<T> {

    /**
     * <p>Title: receive</p>
     * <p>Description: 接收消息</p>
     *
     * @param message 消息
     * @throws MQException MQ异常
     */
    void receive(T message) throws MQException;

    /**
     * <p>Title: getConsumerKey</p>
     * <p>Description: 消费者标识</p>
     *
     * @return 消费者标识
     * @throws MQException MQ异常
     */
    String getConsumerKey() throws MQException;
}
