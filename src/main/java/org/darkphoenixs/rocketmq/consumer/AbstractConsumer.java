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

package org.darkphoenixs.rocketmq.consumer;

import org.darkphoenixs.mq.consumer.MQConsumer;
import org.darkphoenixs.mq.exception.MQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>Title: AbstractConsumer</p>
 * <p>Description: 消费者抽象类</p>
 *
 * @param <T> the type parameter
 * @author Victor
 * @version 1.0
 * @see MQConsumer
 * @since 2017 /12/10
 */
public abstract class AbstractConsumer<T> implements MQConsumer<T> {

    /**
     * The Logger.
     */
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private String consumerKey;

    /**
     * Sets consumer key.
     *
     * @param consumerKey the consumer key
     */
    public void setConsumerKey(String consumerKey) {
        this.consumerKey = consumerKey;
    }

    @Override
    public String getConsumerKey() {
        return consumerKey;
    }

    @Deprecated
    public void receive(T message) throws MQException {

        try {
            doReceive(message);

        } catch (Exception e) {

            throw new MQException(e);
        }

        logger.debug("Receive Success, Message : " + message);
    }

    /**
     * Receive.
     *
     * @param messages the messages
     * @throws MQException the mq exception
     */
    @Deprecated
    public void receive(List<T> messages) throws MQException {

        try {
            doReceive(messages);

        } catch (Exception e) {

            throw new MQException(e);
        }

        logger.debug("Receive Success, Message size: " + messages.size());
    }

    /**
     * Receive.
     *
     * @param key     the key
     * @param message the message
     * @throws MQException the mq exception
     */
    public void receive(String key, T message) throws MQException {

        try {
            doReceive(key, message);

        } catch (Exception e) {

            throw new MQException(e);
        }

        logger.debug("Receive Success, Message : " + message);
    }

    /**
     * Receive.
     *
     * @param messages the messages
     * @throws MQException the mq exception
     */
    public void receive(Map<String, T> messages) throws MQException {

        try {
            doReceive(messages);

        } catch (Exception e) {

            throw new MQException(e);
        }

        logger.debug("Receive Success, Message size: " + messages.size());
    }

    /**
     * Do receive.
     *
     * @param message the message
     * @throws MQException the mq exception
     */
    protected void doReceive(T message) throws MQException {
        // to Override
    }

    /**
     * Do receive.
     *
     * @param messages the messages
     * @throws MQException the mq exception
     */
    protected void doReceive(List<T> messages) throws MQException {
        // to Override
    }

    /**
     * Do receive.
     *
     * @param key     the key
     * @param message the message
     * @throws MQException the mq exception
     */
    protected void doReceive(String key, T message) throws MQException {

        doReceive(message);
    }

    /**
     * Do receive.
     *
     * @param messages the messages
     * @throws MQException the mq exception
     */
    protected void doReceive(Map<String, T> messages) throws MQException {

        doReceive(new ArrayList<T>(messages.values()));
    }
}
