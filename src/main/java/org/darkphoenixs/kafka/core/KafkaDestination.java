/**
 * <p>Title: KafkaDestination.java</p>
 * <p>Description: KafkaDestination</p>
 * <p>Package: org.darkphoenixs.kafka.core</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization) 2015</p>
 */
package org.darkphoenixs.kafka.core;

/**
 * <p>Title: KafkaDestination</p>
 * <p>Description: Kafka队列对象</p>
 *
 * @since 2015年9月12日
 * @author Victor
 * @version 1.0
 */
public class KafkaDestination {

	/** destinationName */
	private String destinationName;
	
	/**
	 * <p>Title: KafkaDestination</p>
	 * <p>Description: KafkaDestination</p>
	 */
	public KafkaDestination() {
	}
	
	/**
	 * <p>Title: KafkaDestination</p>
	 * <p>Description: KafkaDestination</p>
	 *
	 * @param destinationName destinationName
	 */
	public KafkaDestination(String destinationName) {
		this.destinationName = destinationName;
	}

	/**
	 * @return the destinationName
	 */
	public String getDestinationName() {
		return destinationName;
	}

	/**
	 * @param destinationName the destinationName to set
	 */
	public void setDestinationName(String destinationName) {
		this.destinationName = destinationName;
	}

}
