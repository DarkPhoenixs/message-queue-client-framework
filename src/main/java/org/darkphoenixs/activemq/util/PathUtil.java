package org.darkphoenixs.activemq.util;

import java.io.File;

/**
 * <p>Title: PathUtil</p>
 * <p>Description: 路径工具类</p>
 *
 * @since MessageQueueClient
 * @author Victor.Zxy
 * @version 1.0
 */
public class PathUtil {

	/** 路径分隔符 */
	public static final String SEPARATOR = File.separator;

	/** 当前路径 */
	public static final String PATH = System.getProperty("user.dir");

	/** 配置文件目录 */
	public static final String CONF = "conf";

	/** 配置文件路径 */
	public static final String CONF_PATH = SEPARATOR + PATH + SEPARATOR + CONF
			+ SEPARATOR;

}
