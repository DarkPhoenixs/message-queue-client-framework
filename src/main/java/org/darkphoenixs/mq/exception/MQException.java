/**
 * <p>Title: MQException.java</p>
 * <p>Description: MQException</p>
 * <p>Package: org.darkphoenixs.mq.exception</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.mq.exception;

/**
 * <p>Title: MQException</p>
 * <p>Description: MQ异常类</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see Exception
 * @version 1.0
 */
public class MQException extends Exception {

	private static final long serialVersionUID = 3423882579546941370L;

	public MQException() {
		super();
	}

	public MQException(String message) {
		super(message);
	}

	public MQException(Throwable cause) {
		super(cause);
	}

	public MQException(String message, Throwable cause) {
		super(message, cause);
	}
}
