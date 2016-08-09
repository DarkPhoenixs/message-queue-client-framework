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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class KafkaMessageDecoderImpl extends
        KafkaMessageDecoder<Integer, MessageBeanImpl> {

    @Override
    public Integer decodeKey(byte[] bytes) throws MQException {

        if (bytes != null)

            return Integer.valueOf(new String(bytes));

        return null;
    }

    @Override
    public MessageBeanImpl decodeVal(byte[] bytes) throws MQException {

        MessageBeanImpl message = null;

        ByteArrayInputStream bis = null;

        ObjectInputStream ois = null;

        try {
            bis = new ByteArrayInputStream(bytes);

            ois = new ObjectInputStream(bis);

            message = (MessageBeanImpl) ois.readObject();

        } catch (Exception e) {

            throw new MQException(e);

        } finally {

            try {
                if (ois != null)
                    ois.close();

                if (bis != null)
                    bis.close();

            } catch (Exception e) {
                throw new MQException(e);
            }
        }

        return message;
    }

    @Override
    public Map<Integer, MessageBeanImpl> batchDecode(Map<byte[], byte[]> bytes)
            throws MQException {

        Map<Integer, MessageBeanImpl> map = new HashMap<Integer, MessageBeanImpl>();

        if (bytes != null)

            for (Entry<byte[], byte[]> entry : bytes.entrySet())

                map.put(decodeKey(entry.getKey()), decodeVal(entry.getValue()));

        return map;
    }

}
