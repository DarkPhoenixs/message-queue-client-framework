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
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class MQMessageEncoderAdapterTest {

    @Test
    public void batchEncode() throws Exception {

        Assert.assertArrayEquals("abc".getBytes("UTF-8"), encoderAdapter.encode("abc"));
        Assert.assertArrayEquals("哈哈".getBytes("UTF-8"), encoderAdapter.encode("哈哈"));

        List<String> list = new ArrayList<String>();
        list.add("啦啦");
        list.add("哈哈");

        Assert.assertArrayEquals("啦啦".getBytes("UTF-8"), encoderAdapter.batchEncode(list).get(0));
        Assert.assertArrayEquals("哈哈".getBytes("UTF-8"), encoderAdapter.batchEncode(list).get(1));
    }

    private MQMessageEncoderAdapter<String> encoderAdapter = new MQMessageEncoderAdapter<String>() {
        @Override
        public byte[] encode(String message) throws MQException {

            try {
                return message.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            return null;
        }
    };
}