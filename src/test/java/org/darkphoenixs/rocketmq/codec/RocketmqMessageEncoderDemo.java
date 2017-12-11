/*
 * Copyright (c) 2017. Dark Phoenixs (Open-Source Organization).
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

package org.darkphoenixs.rocketmq.codec;

import org.darkphoenixs.mq.exception.MQException;

import java.util.ArrayList;
import java.util.List;

public class RocketmqMessageEncoderDemo extends RocketmqMessageEncoder<String> {

    public byte[] encode(String message) throws MQException {
        return message.getBytes();
    }

    public List<byte[]> batchEncode(List<String> message) throws MQException {

        List<byte[]> bytes = new ArrayList<byte[]>();

        for (String string : message)

            bytes.add(string.getBytes());

        return bytes;
    }
}
