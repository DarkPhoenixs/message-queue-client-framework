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

/**
 * <p>Title: KafkaMessagNewSenderPool</p>
 * <p>Description: Kafka消息发送连接池（连接单例）</p>
 *
 * @param <K> the type of message key
 * @param <V> the type of message value
 * @author Victor.Zxy
 * @version 1.4.0
 * @see MessageSenderPool
 * @since 2016 /7/26
 */
public class KafkaMessagNewSenderPool<K, V> implements MessageSenderPool<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessagNewSenderPool.class);

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
    protected KafkaMessageNewSender sender;

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
            logger.error(e.getMessage());
        }
    }

    @Override
    public synchronized void init() {

        logger.info("Message sender pool initializing. KafkaProducer config : " + props.toString());

        sender = KafkaMessageNewSender.getOrCreateInstance(props);
    }

    @Override
    public synchronized void destroy() {

        logger.info("Message sender pool closing.");

        sender.shutDown();
    }

    @Override
    public KafkaMessageSender<K, V> getSender() {

        KafkaMessageNewSender<K, V> sender = KafkaMessageNewSender.getOrCreateInstance(props);

        return sender;
    }

    @Override
    public void returnSender(KafkaMessageSender<K, V> sender) {

        sender.close();
    }
}
