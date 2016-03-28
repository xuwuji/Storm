package com.xuwuji.stock.kafka;

import kafka.producer.Partitioner;

/**
 * 
 * @author wuxu 2016-3-16
 *
 *         This class customize a partitioner, it takes the key as the logic how
 *         to partition messages
 */
public class CustomPatitioner implements Partitioner {

	/*
	 * Key: partition based on the key
	 * 
	 * partitionNum: the number of partitions
	 * 
	 * the partitionNum can be declared in the console
	 * 
	 * @see kafka.producer.Partitioner#partition(java.lang.Object, int)
	 */
	public int partition(Object key, int partitionNum) {
		int partition = 0;
		String partitionKey = (String) key;
		// logic comes here,for example
		if (partitionKey.length() > 10) {
			partition = 2 % partitionNum;
		} else {
			partition = 1 % partitionNum;
		}
		return partition;
	}
}
