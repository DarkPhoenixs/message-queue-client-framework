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
package org.darkphoenixs.mq.common;

import org.darkphoenixs.mq.consumer.MQConsumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.MQConsumerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>Title: MQMessageConsumerFactory</p>
 * <p>Description: 消息消费者工厂</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @see MQConsumerFactory
 * @since 2015-06-01
 */
public class MQMessageConsumerFactory implements MQConsumerFactory {

    /**
     * instance
     */
    private static final AtomicReference<MQMessageConsumerFactory> instance = new AtomicReference<MQMessageConsumerFactory>();
    /**
     * logger
     */
    protected Logger logger = LoggerFactory.getLogger(MQMessageConsumerFactory.class);
    /**
     * consumers
     */
    private MQConsumer<?>[] consumers;

    /**
     * consumerCache
     */
    private ConcurrentHashMap<String, MQConsumer<?>> consumerCache = new ConcurrentHashMap<String, MQConsumer<?>>();

    /**
     * private construction method
     */
    private MQMessageConsumerFactory() {
    }

    /**
     * get singleton instance method
     */
    public synchronized static MQConsumerFactory getInstance() {

        if (instance.get() == null)
            instance.set(new MQMessageConsumerFactory());
        return instance.get();
    }

    /**
     * @param consumers the consumers to set
     */
    public void setConsumers(MQConsumer<?>[] consumers) {
        this.consumers = consumers;
    }

    @Override
    public <T> void addConsumer(MQConsumer<T> consumer) throws MQException {

        consumerCache.put(consumer.getConsumerKey(), consumer);

        logger.debug("Add MQConsumer : " + consumer.getConsumerKey());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> MQConsumer<T> getConsumer(String consumerKey) throws MQException {

        if (consumerCache.containsKey(consumerKey)) {

            logger.debug("Get MQConsumer : " + consumerKey);

            return (MQConsumer<T>) consumerCache.get(consumerKey);

        } else {

            logger.warn("Unknown ConsumerKey : " + consumerKey);

            return null;
        }
    }

    @Override
    public void init() throws MQException {

        if (consumers != null)

            for (int i = 0; i < consumers.length; i++)

                consumerCache.put(consumers[i].getConsumerKey(), consumers[i]);

        logger.debug("Initialized!");

    }

    @Override
    public void destroy() throws MQException {

        if (consumers != null)
            consumers = null;

        if (instance.get() != null)
            instance.set(null);

        consumerCache.clear();

        logger.debug("Destroyed!");
    }

}
