/*
 * Copyright (c) 2017. Dark Phoenixs (Open-Source Organization).
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

package org.darkphoenixs.rocketmq.listener;

import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.listener.MQMessageListener;

import java.util.List;
import java.util.Map;

/**
 * <p>Title: RocketmqMessageListener</p>
 * <p>Description: Rocketmq消息监听器抽象类</p>
 *
 * @param <T> the type parameter
 * @author Victor
 * @version 1.0
 * @see MQMessageListener
 * @since 2017 /12/10
 */
public abstract class RocketmqMessageListener<T> implements MQMessageListener<T> {

    /**
     * On message.
     *
     * @param messages the messages
     * @throws MQException the mq exception
     */
    @Deprecated
    public abstract void onMessage(final List<T> messages) throws MQException;

    /**
     * On message.
     *
     * @param key     the key
     * @param message the message
     * @throws MQException the mq exception
     */
    public abstract void onMessage(final String key, final T message) throws MQException;

    /**
     * On message.
     *
     * @param messages the messages
     * @throws MQException the mq exception
     */
    public abstract void onMessage(final Map<String, T> messages) throws MQException;

    /**
     * Gets message listener.
     *
     * @return the message listener
     */
    public abstract MessageListener getMessageListener();
}
