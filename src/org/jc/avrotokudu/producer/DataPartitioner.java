/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.avrotokudu.producer;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 *
 * @author cespedjo
 */
public class DataPartitioner implements Partitioner {
    public DataPartitioner(VerifiableProperties props) {
        System.out.println("Creating partitioner");
    }
 
    @Override
    public int partition(Object key, int a_numPartitions) {
        String stringKey = (String) key;
        //Perform some calculation based on the key received.
        int partition = Integer.parseInt( stringKey.substring(stringKey.length() - 2)) - 1;
        
        return partition;
  }
}
