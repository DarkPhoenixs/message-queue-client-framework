/*
 * Copyright (c) 2018. Dark Phoenixs (Open-Source Organization).
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The type Mq consumer adapter.
 *
 * @param <T> the type parameter
 */
public abstract class MQConsumerAdapter<T> implements MQConsumer<T> {

    /**
     * The Logger.
     */
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private String consumerKey;

    @Override
    public String getConsumerKey() {
        return consumerKey;
    }

    /**
     * Sets consumer key.
     *
     * @param consumerKey the consumer key
     */
    public void setConsumerKey(String consumerKey) {
        this.consumerKey = consumerKey;
    }

    @Override
    public void receive(T message) throws MQException {

        try {
            doReceive(message);

        } catch (Exception e) {

            throw new MQException(e);
        }

        logger.debug("Receive Success, ConsumerKey : " + this.getConsumerKey()
                + " , Message : " + message);
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

        logger.debug("Receive Success, ConsumerKey : " + this.getConsumerKey()
                + " , Key : " + key
                + " , Message : " + message);
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

        logger.debug("Receive Success, ConsumerKey : " + this.getConsumerKey()
                + " , Messages size: " + messages.size());
    }

    /**
     * Do receive.
     *
     * @param message the message
     * @throws MQException the mq exception
     */
    protected abstract void doReceive(T message) throws MQException;


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
    protected void doReceive(List<T> messages) throws MQException {

        for (T message : messages)

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
