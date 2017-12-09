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
package org.darkphoenixs.mq.listener;

import org.darkphoenixs.mq.exception.MQException;

/**
 * <p>Title: MQMessageListener</p>
 * <p>Description: 消息监听器接口</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @since 2015-06-01
 */
public interface MQMessageListener<T> {

    /**
     * <p>Title: onMessage</p>
     * <p>Description: 监听方法</p>
     *
     * @param message 消息
     * @throws MQException MQ异常
     */
    void onMessage(final T message) throws MQException;
}
