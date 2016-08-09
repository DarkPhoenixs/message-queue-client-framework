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

import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.message.MessageBeanImpl;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class KafkaMessageEncoderImpl extends
        KafkaMessageEncoder<Integer, MessageBeanImpl> {

    @Override
    public byte[] encodeKey(Integer key) throws MQException {

        if (key != null)

            return String.valueOf(key).getBytes();

        return null;
    }

    @Override
    public byte[] encodeVal(MessageBeanImpl val) throws MQException {

        ByteArrayOutputStream bos = null;

        ObjectOutputStream oos = null;

        byte[] bytes = null;

        try {
            bos = new ByteArrayOutputStream();

            oos = new ObjectOutputStream(bos);

            oos.writeObject(val);

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
    public Map<byte[], byte[]> batchEncode(
            Map<Integer, MessageBeanImpl> messages) throws MQException {

        Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();

        if (messages != null)

            for (Entry<Integer, MessageBeanImpl> entry : messages.entrySet())

                map.put(encodeKey(entry.getKey()), encodeVal(entry.getValue()));

        return map;
    }
}
