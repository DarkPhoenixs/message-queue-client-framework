/**
 * <p>Title: KafkaPoolThreadFactory.java</p>
 * <p>Description: KafkaPoolThreadFactory</p>
 * <p>Package: org.darkphoenixs.kafka.pool</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization) 2016</p>
 */
package org.darkphoenixs.kafka.pool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>Title: KafkaPoolThreadFactory</p>
 * <p>Description: Kafka池化线程工厂</p>
 *
 * @since 2016年4月28日
 * @author Victor.Zxy
 * @see ThreadFactory
 * @version 1.0
 */
public class KafkaPoolThreadFactory implements ThreadFactory {

	/** 线程计数器 */
	private AtomicInteger i = new AtomicInteger(0);

	/** 线程前缀 */
	private String prefix = "KafkaPool";

	/** 线程优先级 */
	private int priority;

	/** 守护线程 */
	private boolean daemon;
	
	/**
	 * 默认构造方法
	 */
	public KafkaPoolThreadFactory() {
		
	}
	
	/**
	 * 构造方法初始化
	 * 
	 * @param prefix 线程前缀
	 */
	public KafkaPoolThreadFactory(String prefix) {
		this.prefix = prefix;
	}

	/**
	 * 构造方法初始化
	 *
	 * @param prefix 线程前缀
	 * @param daemon 是否守护线程
	 */
	public KafkaPoolThreadFactory(String prefix, boolean daemon) {
		this.prefix = prefix;
		this.daemon = daemon;
	}
	
	/**
	 * 构造方法初始化
	 *
	 * @param priority 线程优先级
	 * @param daemon 是否守护线程
	 */
	public KafkaPoolThreadFactory(int priority, boolean daemon) {
		this.priority = priority;
		this.daemon = daemon;
	}

	/**
	 * 构造方法初始化
	 *
	 * @param prefix 线程前缀
	 * @param priority 线程优先级
	 * @param daemon 是否守护线程
	 */
	public KafkaPoolThreadFactory(String prefix, int priority, boolean daemon) {
		this.prefix = prefix;
		this.priority = priority;
		this.daemon = daemon;
	}

	@Override
	public Thread newThread(Runnable r) {
		
		Thread thread = new Thread(r);

		if (priority >= 1 && priority <= 10)
			
			thread.setPriority(priority);

		thread.setDaemon(daemon);

		thread.setName(prefix + "-" + i.getAndIncrement());

		return thread;
	}

	/**
	 * @return the prefix
	 */
	public String getPrefix() {
		return prefix;
	}

	/**
	 * @param prefix
	 *            the prefix to set
	 */
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	/**
	 * @return the priority
	 */
	public int getPriority() {
		return priority;
	}

	/**
	 * @param priority
	 *            the priority to set
	 */
	public void setPriority(int priority) {
		this.priority = priority;
	}

	/**
	 * @return the daemon
	 */
	public boolean isDaemon() {
		return daemon;
	}

	/**
	 * @param daemon
	 *            the daemon to set
	 */
	public void setDaemon(boolean daemon) {
		this.daemon = daemon;
	}
}
