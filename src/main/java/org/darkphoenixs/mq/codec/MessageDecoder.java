/**
 * <p>Title: MessageDecoder.java</p>
 * <p>Description: MessageDecoder</p>
 * <p>Package: org.darkphoenixs.mq.codec</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.mq.codec;

import java.util.List;

import org.darkphoenixs.mq.exception.MQException;

/**
 * <p>Title: MessageDecoder</p>
 * <p>Description: 消息解码器接口</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public interface MessageDecoder<T> {

	/**
	 * <p>Title: decode</p>
	 * <p>Description: 消息反序列化</p>
	 *
	 * @param bytes 消息
	 * @return 反序列化消息
	 * @throws MQException
	 */
	T decode(byte[] bytes) throws MQException;
	
	/**
	 * <p>Title: batchDecode</p>
	 * <p>Description: 批量反序列化</p>
	 *
	 * @param bytes 消息列表
	 * @return 反序列化列表
	 * @throws MQException
	 */
	List<T> batchDecode(List<byte[]> bytes) throws MQException;

}
