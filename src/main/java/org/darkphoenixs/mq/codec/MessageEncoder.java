/**
 * <p>Title: MessageEncoder.java</p>
 * <p>Description: MessageEncoder</p>
 * <p>Package: org.darkphoenixs.mq.codec</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.mq.codec;

import java.util.List;

import org.darkphoenixs.mq.exception.MQException;

/**
 * <p>Title: MessageEncoder</p>
 * <p>Description: 消息编码器接口</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public interface MessageEncoder<T> {

	/**
	 * <p>Title: encode</p>
	 * <p>Description: 消息序列化</p>
	 *
	 * @param message 消息
	 * @return 消息序列化
	 * @throws MQException
	 */
	byte[] encode(T message) throws MQException;
	
	/**
	 * <p>Title: batchEncode</p>
	 * <p>Description: 批量序列化</p>
	 *
	 * @param message 消息列表
	 * @return 消息序列化列表
	 * @throws MQException
	 */
	List<byte[]> batchEncode(List<T> message) throws MQException;
}
