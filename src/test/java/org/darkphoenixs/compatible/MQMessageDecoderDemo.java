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

package org.darkphoenixs.compatible;

import org.darkphoenixs.mq.codec.MQMessageDecoder;
import org.darkphoenixs.mq.exception.MQException;

import java.util.ArrayList;
import java.util.List;

public class MQMessageDecoderDemo implements MQMessageDecoder<String> {

    @Override
    public String decode(byte[] bytes) throws MQException {
        return new String(bytes);
    }

    @Override
    public List<String> batchDecode(List<byte[]> bytes) throws MQException {
        List<String> list = new ArrayList<String>();
        for (byte[] b : bytes)
            list.add(decode(b));
        return list;
    }
}
