/**
 * 
 */
package com.sapient;

import org.apache.spark.Partitioner;

/**
 * @author aku375
 *
 */
public class CharRangePartition extends Partitioner{
	
	private Integer numOfPartitions;
	

	public CharRangePartition(Integer numOfPartitions) {
		super();
		this.numOfPartitions = numOfPartitions;
	}

	@Override
	public int getPartition(Object arg0) {
		String stockName = (String)arg0;
		
		return Character.hashCode(stockName.charAt(0)) % numPartitions();
	}

	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return this.numOfPartitions;
	}

}
