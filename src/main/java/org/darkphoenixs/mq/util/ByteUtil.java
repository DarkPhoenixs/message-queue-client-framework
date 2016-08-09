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
package org.darkphoenixs.mq.util;

/**
 * <p>Title: ByteUtil</p>
 * <p>Description: 字节数组工具类</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @since 2015-06-01
 */
public class ByteUtil {

    /**
     * 合并数组
     *
     * @param b1 数组1
     * @param b2 数组2
     * @return 合并后的数组
     */
    public static byte[] merge(byte[] b1, byte[] b2) {

        byte[] merge = new byte[b1.length + b2.length];

        System.arraycopy(b1, 0, merge, 0, b1.length);

        System.arraycopy(b2, 0, merge, b1.length, b2.length);

        return merge;
    }

    /**
     * 数组截取
     *
     * @param bytes 数组
     * @param begin 起始位置
     * @param end   结束位置
     * @return 截取后的数组
     */
    public static byte[] sub(byte[] bytes, int begin, int end) {

        byte[] sub = new byte[end - begin];

        for (int i = 0; i < sub.length; i++) {

            sub[i] = bytes[begin + i];
        }

        return sub;
    }
}
