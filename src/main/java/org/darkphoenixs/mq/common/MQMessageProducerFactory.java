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

import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.MQProducerFactory;
import org.darkphoenixs.mq.producer.MQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>Title: MQMessageProducerFactory</p>
 * <p>Description: 消息生产者工厂</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @see MQProducerFactory
 * @since 2015-06-01
 */
public class MQMessageProducerFactory implements MQProducerFactory {

    /**
     * instance
     */
    private static final AtomicReference<MQMessageProducerFactory> instance = new AtomicReference<MQMessageProducerFactory>();
    /**
     * logger
     */
    protected Logger logger = LoggerFactory.getLogger(MQMessageProducerFactory.class);
    /**
     * producers
     */
    private MQProducer<?>[] producers;

    /**
     * producerCache
     */
    private ConcurrentHashMap<String, MQProducer<?>> producerCache = new ConcurrentHashMap<String, MQProducer<?>>();

    /**
     * private construction method
     */
    private MQMessageProducerFactory() {
    }

    /**
     * get singleton instance method
     */
    public synchronized static MQProducerFactory getInstance() {

        if (instance.get() == null)
            instance.set(new MQMessageProducerFactory());
        return instance.get();
    }

    /**
     * @param producers the producers to set
     */
    public void setProducers(MQProducer<?>[] producers) {
        this.producers = producers;
    }

    @Override
    public <T> void addProducer(MQProducer<T> producer) throws MQException {

        producerCache.put(producer.getProducerKey(), producer);

        logger.debug("Add MQProducer : " + producer.getProducerKey());

    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> MQProducer<T> getProducer(String producerKey) throws MQException {

        if (producerCache.containsKey(producerKey)) {

            logger.debug("Get MQProducer : " + producerKey);

            return (MQProducer<T>) producerCache.get(producerKey);

        } else {

            logger.warn("Unknown ProducerKey : " + producerKey);

            return null;
        }
    }

    @Override
    public void init() throws MQException {

        if (producers != null)

            for (int i = 0; i < producers.length; i++)

                producerCache.put(producers[i].getProducerKey(), producers[i]);

    }

    @Override
    public void destroy() throws MQException {

        if (producers != null)
            producers = null;

        if (instance.get() != null)
            instance.set(null);

        producerCache.clear();

        logger.debug("Destroyed!");
    }
}
