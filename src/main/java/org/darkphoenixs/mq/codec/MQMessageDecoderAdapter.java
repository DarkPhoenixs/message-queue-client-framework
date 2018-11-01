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
package org.darkphoenixs.mq.codec;

import org.darkphoenixs.mq.exception.MQException;

import java.util.ArrayList;
import java.util.List;

/**
 * The type Mq message decoder adapter.
 *
 * @param <T> the type parameter
 * @author Victor.Zxy
 * @since 1.5.7
 */
public abstract class MQMessageDecoderAdapter<T> implements MQMessageDecoder<T> {

    @Override
    public List<T> batchDecode(List<byte[]> bytes) throws MQException {

        List<T> list = new ArrayList<T>();

        for (byte[] b : bytes)

            list.add(decode(b));

        return list;
    }
}
