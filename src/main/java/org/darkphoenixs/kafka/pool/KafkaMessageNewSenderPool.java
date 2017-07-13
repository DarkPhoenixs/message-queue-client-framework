/*
 * Copyright (c) 2016. Dark Phoenixs (Open-Source Organization).
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

package org.darkphoenixs.kafka.pool;

import org.darkphoenixs.kafka.core.KafkaMessageNewSender;
import org.darkphoenixs.kafka.core.KafkaMessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>Title: KafkaMessageNewSenderPool</p>
 * <p>Description: 新Kafka消息发送连接池</p>
 * <p>
 * <p>连接采用线程安全单例模式</p>
 *
 * @param <K> the type of kafka message key
 * @param <V> the type of kafka message value
 * @author Victor.Zxy
 * @version 1.4.0
 * @see MessageSenderPool
 * @since 2016 /7/26
 */
public class KafkaMessageNewSenderPool<K, V> implements MessageSenderPool<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageNewSenderPool.class);

    /**
     * The Running.
     */
    protected AtomicBoolean running = new AtomicBoolean(false);

    /**
     * The Props.
     */
    protected Properties props = new Properties();

    /**
     * The Config.
     */
    protected Resource config;

    /**
     * The Sender.
     */
    protected KafkaMessageSender<K, V> sender;

    /**
     * The Pool size (unused).
     *
     * @deprecated
     */
    protected int poolSize;

    /**
     * Gets pool size.
     *
     * @return the pool size
     * @deprecated
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Sets pool size.
     *
     * @param poolSize the pool size
     * @deprecated
     */
    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    /**
     * Gets props.
     *
     * @return the props
     */
    public Properties getProps() {
        return props;
    }

    /**
     * Sets props.
     *
     * @param props the props
     */
    public void setProps(Properties props) {
        this.props = props;
    }

    /**
     * Gets config.
     *
     * @return the config
     */
    public Resource getConfig() {
        return config;
    }

    /**
     * Sets config.
     *
     * @param config the config
     */
    public void setConfig(Resource config) {
        this.config = config;
        try {
            PropertiesLoaderUtils.fillProperties(props, this.config);
        } catch (IOException e) {
            logger.error("Fill properties failed.", e);
        }
    }

    @Override
    public synchronized void init() {

        sender = new KafkaMessageNewSender<K, V>(props);

        running.set(true);

        logger.info("Message Sender Pool initialized.");
    }

    @Override
    public synchronized void destroy() {

        sender.shutDown();

        running.set(false);

        logger.info("Message Sender pool closed.");
    }

    @Override
    public synchronized boolean isRunning() {

        return running.get();
    }

    @Override
    public KafkaMessageSender<K, V> getSender() {

        if (sender == null)

            sender = new KafkaMessageNewSender<K, V>(props);

        return sender;
    }

    @Override
    public void returnSender(KafkaMessageSender<K, V> sender) {

        // nothing to do.
    }
}
