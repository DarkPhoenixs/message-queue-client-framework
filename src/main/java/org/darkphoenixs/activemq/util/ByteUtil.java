package org.darkphoenixs.activemq.util;

/**
 * <p>Title: ByteUtil</p>
 * <p>Description: 字节数组工具类</p>
 *
 * @since MessageQueueClient
 * @author Victor.Zxy
 * @version 1.0
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
	 * @param end 结束位置
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
