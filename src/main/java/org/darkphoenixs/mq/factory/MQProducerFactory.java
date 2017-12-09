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
package org.darkphoenixs.mq.factory;

import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.producer.MQProducer;

/**
 * <p>Title: MQProducerFactory</p>
 * <p>Description: 生产者工厂接口</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @since 2015-06-01
 */
public interface MQProducerFactory {

    /**
     * <p>Title: addProducer</p>
     * <p>Description: 增加生产者</p>
     *
     * @param producer 生产者
     * @throws MQException MQ异常
     */
    <T> void addProducer(MQProducer<T> producer) throws MQException;

    /**
     * <p>Title: getProducer</p>
     * <p>Description: 获得生产者</p>
     *
     * @param producerKey 生产者标识
     * @return 生产者
     * @throws MQException MQ异常
     */
    <T> MQProducer<T> getProducer(String producerKey) throws MQException;

    /**
     * <p>Title: init</p>
     * <p>Description: 初始化工厂</p>
     *
     * @throws MQException MQ异常
     */
    void init() throws MQException;

    /**
     * <p>Title: destroy</p>
     * <p>Description: 销毁工厂</p>
     *
     * @throws MQException MQ异常
     */
    void destroy() throws MQException;
}
