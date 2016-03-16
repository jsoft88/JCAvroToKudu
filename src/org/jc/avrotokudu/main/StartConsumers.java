/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.avrotokudu.main;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jc.avrotokudu.consumer.AvroConsumer;
import org.jc.kafka.customexecutor.KafkaThreadPoolExecutor;

/**
 *
 * @author Jorge Cespedes
 * 
 * Main class to launch consumers. Its purpose is to consume json messages from
 * kafka partitions and insert them into a kudu table.
 * 
 * Usage StartConsumers maxNumberOfConsumers consumerGroup topic consumerPort \
 *      zookeeper:port commaSeparatedBrokers maxRetriesOnEmptyPartition kuduTableName \
 *      masterAddressesCommaSeparated
 * 
 * maxNumberOfConsumers sets the number of consumers to use, this would normally
 * match the number of partitions available under a topic. 
 * ConsumerGroup is the name of the consumer group, could be anything.
 * Topic, the topic containing the partitions from which messages are to be read
 * ConsumerPort, port number of consumers
 * Zookeeper:port, zookeeper host address followed by colon and port number
 * CommaSeparatedBrokers: brokers addresses (without port number). Please list all
 * of the available brokers and DO NOT INCLUDE SPACES.
 * MaxRetriesOnEmptyPartition, how many times would a consumer retry to obtain 
 * a message from a partition when none is available.
 * KuduTableName, name of the kudu table where records will be inserted.
 * masterAddressesCommaSeparated, comma-separated kudu master addresse. DO NOT 
 * INCLUDE SPACES.
 * 
 */
public class StartConsumers {
    
    private static final String DELIMITER = ",";
    
    private static final int SO_TIMEOUT_CONSUMER = 100000;
    
    //Countdown for waiting until consumers are done.
    private static CountDownLatch consumerCountDown;
    
    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            System.out.println("Usage: StartConsumers maxNumberOfConsumers consumerGroup topic" +
                    " consumerPort zookeeper:port commaSeparatedBrokers");
            return;
        }
        
        if (args.length < 9) {
            try {
                System.out.println(new PrintHelp().getHelpText(PrintHelp.HELP_FILE_CONSUMER));
                return;
            } catch (IOException ex) {
                Logger.getLogger(StartConsumers.class.getName()).log(Level.SEVERE, null, ex);
                return;
            }
        }
        
        final int maxNumberOfConsumers = Integer.parseInt(args[0]);
        final String consumerGroup = args[1];
        final String topic = args[2];
        final int consumerPort = Integer.parseInt(args[3]);
        final String zookeeperHost = args[4];
        final String commaSeparatedBrokers = args[5];
        final int maxRetriesEmptyPartition = Integer.parseInt(args[6]);
        final String kuduTable = args[7];
        final String commaSeparatedMasters = args[8];
        
        StartConsumers.consumerCountDown = new CountDownLatch(maxNumberOfConsumers);
        
        ExecutorService consumerThreadPool = 
                new KafkaThreadPoolExecutor(maxNumberOfConsumers, maxNumberOfConsumers, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        
        
        final Properties consumerProperties = new Properties();
        
        consumerProperties.put("zookeeper.connect", zookeeperHost);
        consumerProperties.put("group.id", consumerGroup);
        //Hard-coded default properties
        consumerProperties.put("zookeeper.session.timeout.ms", "2000");
        consumerProperties.put("zookeeper.sync.time.ms", "200");
        consumerProperties.put("auto.commit.interval.ms", "1000");
        
        long startTimeConsumers = System.currentTimeMillis();
        
        for (int i = 0; i < maxNumberOfConsumers; ++i) {
            final int partitionToConsume = i;
            consumerThreadPool.submit(new Runnable() {

                @Override
                public void run() {
                    AvroConsumer consumer = 
                            new AvroConsumer(consumerGroup, 
                                            topic, 
                                            partitionToConsume, 
                                            commaSeparatedBrokers.split(DELIMITER), 
                                            SO_TIMEOUT_CONSUMER, 
                                            consumerPort, 
                                            maxRetriesEmptyPartition,
                                            kuduTable,
                                            commaSeparatedMasters.split(DELIMITER));
                    
                    consumer.consume();
                    consumerCountDown.countDown();
                }
            });
        }
        
        try {
            consumerCountDown.await();
            System.out.println("Consumers are done. Total time: " + (System.currentTimeMillis() - startTimeConsumers) / 1000 + " seconds");
        } catch (InterruptedException ex) {
            Logger.getLogger(StartConsumers.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            consumerThreadPool.shutdown();
        }
    }
}
