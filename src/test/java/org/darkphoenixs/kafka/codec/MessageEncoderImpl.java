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
package org.darkphoenixs.kafka.codec;

import org.darkphoenixs.mq.codec.MQMessageEncoder;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.message.MessageBeanImpl;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Title: MessageEncoderImpl</p>
 * <p>Description: 消息编码器</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @see MQMessageEncoder
 * @since 2015-06-01
 */
public class MessageEncoderImpl implements MQMessageEncoder<MessageBeanImpl> {

    @Override
    public byte[] encode(MessageBeanImpl message) throws MQException {

        ByteArrayOutputStream bos = null;

        ObjectOutputStream oos = null;

        byte[] bytes = null;

        try {
            bos = new ByteArrayOutputStream();

            oos = new ObjectOutputStream(bos);

            oos.writeObject(message);

            bytes = bos.toByteArray();

        } catch (Exception e) {

            throw new MQException(e);

        } finally {

            try {
                if (oos != null)
                    oos.close();

                if (bos != null)
                    bos.close();

            } catch (Exception e) {
                throw new MQException(e);
            }
        }

        return bytes;
    }

    @Override
    public List<byte[]> batchEncode(List<MessageBeanImpl> message)
            throws MQException {

        List<byte[]> list = new ArrayList<byte[]>();

        for (MessageBeanImpl messageBean : message)

            list.add(this.encode(messageBean));

        return list;
    }

}
