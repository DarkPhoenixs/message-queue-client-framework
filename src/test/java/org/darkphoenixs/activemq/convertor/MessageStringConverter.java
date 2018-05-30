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
package org.darkphoenixs.activemq.convertor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.support.converter.MessageConversionException;
import org.springframework.jms.support.converter.MessageConverter;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * <p>Title: MessageConverterImpl</p>
 * <p>Description: 消息转换器实现类 </p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @see MessageConverter
 * @since 2015-06-01
 */
public class MessageStringConverter implements MessageConverter {

    /**
     * 日志对象
     */
    public static final Logger logger = LoggerFactory
            .getLogger(MessageStringConverter.class);

    @Override
    public Message toMessage(Object obj, Session session) throws JMSException,
            MessageConversionException {

        logger.debug("Method : toMessage");

        TextMessage textMessage = session.createTextMessage();

        textMessage.setText((String) obj);

        logger.debug("Convert Success, The Send Message is " + obj);

        return textMessage;
    }

    @Override
    public Object fromMessage(Message message) throws JMSException,
            MessageConversionException {

        logger.debug("Method : fromMessage");

        TextMessage textMessage = (TextMessage) message;

        String msg = textMessage.getText();

        logger.debug("Convert Success, The Receive Message is " + msg);

        return msg;
    }
}
