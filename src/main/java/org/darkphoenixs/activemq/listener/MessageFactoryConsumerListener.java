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
package org.darkphoenixs.activemq.listener;

import org.darkphoenixs.mq.consumer.MQConsumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.MQConsumerFactory;
import org.darkphoenixs.mq.listener.MQMessageListener;
import org.darkphoenixs.mq.util.RefleTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * <p>Title: MessageFactoryConsumerListener</p>
 * <p>Description: 消费者工厂监听器</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @see MQMessageListener
 * @since 2015-06-01
 */
public class MessageFactoryConsumerListener<T> implements MQMessageListener<T> {

    /**
     * logger
     */
    protected Logger logger = LoggerFactory.getLogger(MessageFactoryConsumerListener.class);

    /**
     * consumerKeyField
     */
    private String consumerKeyField;

    /**
     * consumerFactory
     */
    private MQConsumerFactory consumerFactory;

    /**
     * threadPool
     */
    private ExecutorService threadPool;

    /**
     * @return the consumerKeyField
     */
    public String getConsumerKeyField() {
        return consumerKeyField;
    }

    /**
     * @param consumerKeyField the consumerKeyField to set
     */
    public void setConsumerKeyField(String consumerKeyField) {
        this.consumerKeyField = consumerKeyField;
    }

    /**
     * @return the consumerFactory
     */
    public MQConsumerFactory getConsumerFactory() {
        return consumerFactory;
    }

    /**
     * @param consumerFactory the consumerFactory to set
     */
    public void setConsumerFactory(MQConsumerFactory consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    /**
     * @return the threadPool
     */
    public ExecutorService getThreadPool() {
        return threadPool;
    }

    /**
     * @param threadPool the threadPool to set
     */
    public void setThreadPool(ExecutorService threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public void onMessage(final T message) throws MQException {

        if (consumerFactory == null)
            throw new MQException("MQConsumerFactory is null !");

        if (consumerKeyField == null)
            throw new MQException("ConsumerKeyField is null !");

        if (message == null)
            throw new MQException("Message is null !");

        final String consumerKey = RefleTool.getMethodValue(message, "get" + consumerKeyField.substring(0, 1).toUpperCase() + consumerKeyField.substring(1));

        if (consumerKey == null)
            throw new MQException("MQConsumer Key is null !");

        final MQConsumer<T> consumer = consumerFactory.getConsumer(consumerKey);

        if (consumer == null)
            throw new MQException("MQConsumer is null !");

        if (threadPool == null)

            consumer.receive(message);

        else
            threadPool.execute(new Runnable() {

                @Override
                public void run() {

                    try {
                        consumer.receive(message);
                    } catch (MQException e) {
                        logger.error("Receive message failed.", e);
                    }
                }
            });
    }
}
