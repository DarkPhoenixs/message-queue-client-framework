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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: AbstractConsumer</p>
 * <p>Description: 消费者抽象类</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @see MQConsumer
 * @since 2015-06-01
 */
@Deprecated
public abstract class AbstractConsumer<T> implements MQConsumer<T> {

    /**
     * logger
     */
    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * consumerKey
     */
    private String consumerKey;

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

    @Override
    public String getConsumerKey() throws MQException {

        return this.consumerKey;
    }

    /**
     * @param consumerKey the consumerKey to set
     */
    public void setConsumerKey(String consumerKey) {
        this.consumerKey = consumerKey;
    }

    /**
     * <p>Title: doReceive</p>
     * <p>Description: 消息接收方法</p>
     *
     * @param message 消息
     * @throws MQException MQ异常
     */
    protected abstract void doReceive(T message) throws MQException;

}
