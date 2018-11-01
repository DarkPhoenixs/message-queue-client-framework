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

public class MQMessageDecoderAdapterTest {

    @Test
    public void batchDecode() throws Exception {

        Assert.assertEquals("abc", decoderAdapter.decode("abc".getBytes("UTF-8")));
        Assert.assertEquals("哈哈", decoderAdapter.decode("哈哈".getBytes("UTF-8")));

        List<byte[]> bytes = new ArrayList<byte[]>();
        bytes.add("啦啦".getBytes("UTF-8"));
        bytes.add("哈哈".getBytes("UTF-8"));

        Assert.assertNotEquals("哈哈", decoderAdapter.batchDecode(bytes).get(0));
        Assert.assertEquals("哈哈", decoderAdapter.batchDecode(bytes).get(1));
    }

    private MQMessageDecoderAdapter<String> decoderAdapter = new MQMessageDecoderAdapter<String>() {

        @Override
        public String decode(byte[] bytes) throws MQException {

            try {
                return new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            return null;
        }
    };
}