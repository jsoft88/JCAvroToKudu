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
import kafka.producer.ProducerConfig;
import org.jc.kafka.customexecutor.KafkaThreadPoolExecutor;
import org.jc.avrotokudu.producer.DelimitedAvroReaderWorker;

/**
 *
 * @author Jorge Cespedes
 * 
 * Main class to start producers. The producers can read each avro data file 
 * in chunks, that is, each producer will be taking a piece of any given file.
 * The size of the chunk each producer will have to process is controlled by
 * the linesToReadPerWorker argument and the number of producers/workers per file
 * is controlled by maxNumOfWorkersPerFile.
 * 
 * This class expects each line from avro file to be encoded as a json string, 
 * therefore it allows to implement a partitioner based on the value of some 
 * json key. The key to be searched inside a json-encoded avro record is indicated
 * by fieldToUseAsPartition.
 * 
 * Provide TWO(2) brokers to be queried at the very beginning. Any two brokers in
 * the cluster. Finally, provide the fully qualified name of the class to be used
 * to tell which partition the current message belongs to. The key to be used is
 * the one provided in fieldToUseAsPartition.
 * 
 * 
 * Usage: StartProducers \
 * numberOfFiles fileInHdfs1,fileInHdfs2,fileInHdfs3,...,fileInHdfsNumOfFiles \
 * fieldToUseAsPartition linesToReadPerWorker maxNumOfWorkersPerFile \
 * broker1,broker2 FQCNOfPartitioner topic
 * 
 * Example: StartProducers 1 hdfs://name.node:8022/user/hive/warehouse/file1.avro \
 * JSON_KEY 100000 100 host1:9092,host2:9092 org.jc.avrotokudu.producer.DataPartitioner \
 * myTopic
 */
public class StartProducers {
    
    private static final String DEFAULT_SEPARATOR = ",";
    
    //Countdown until all producers are done.
    public static CountDownLatch producerCountDown;
    
    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            System.out.println("Usage: StartProducers 1 hdfs://name.node:8022/user/hive/warehouse/file1.avro" +
                    " JSON_KEY 100000 100 host1:9092,host2:9092 org.jc.avrotokudu.producer.DataPartitioner" + 
                    " myTopic");
            return;
        }
        
        if (args.length < 8) {
            try {
                System.out.println(new PrintHelp().getHelpText(PrintHelp.HELP_FILE_PRODUCER));
                return;
            } catch (IOException ex) {
                Logger.getLogger(StartProducers.class.getName()).log(Level.SEVERE, null, ex);
                return;
            }
        }
        
        int numberOfFiles = Integer.parseInt(args[0]);
        String commaSeparatedFiles = args[1];
        String jsonKeyToUseAsDelimiter = args[2];
        int linesToReadPerWorker = Integer.parseInt(args[3]);
        int maxNumOfWorkers = Integer.parseInt(args[4]);
        String twoBrokers = args[5];
        String fullyQualifiedPartitionerName = args[6];
        String topic = args[7];
        
        StartProducers.producerCountDown = new CountDownLatch(numberOfFiles * maxNumOfWorkers);
        
        ExecutorService producerThreadPool = 
                new KafkaThreadPoolExecutor(numberOfFiles * maxNumOfWorkers, 
                        numberOfFiles * maxNumOfWorkers, 0L, 
                        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        
        final Properties props = new Properties();
 
        props.put("metadata.broker.list", twoBrokers);
        props.put("partitioner.class", fullyQualifiedPartitionerName);
        
        //Hard-coded default properties.
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        long startTimeProducer = System.currentTimeMillis();
        final ProducerConfig config = new ProducerConfig(props);
        int currentStartOffset = 0;
        String[] filesToRead = commaSeparatedFiles.split(DEFAULT_SEPARATOR);
        for (int i = 0; i < filesToRead.length; ++i) {
            
            for (int j = 0; j < maxNumOfWorkers; ++j) {
                final String fileToRead = filesToRead[i].trim();
                DelimitedAvroReaderWorker avroReader =
                        new DelimitedAvroReaderWorker(
                                topic,
                                i,
                                currentStartOffset,
                                currentStartOffset + linesToReadPerWorker,
                                fileToRead,
                                jsonKeyToUseAsDelimiter,
                                config);
                producerThreadPool.submit(avroReader);
                currentStartOffset += (linesToReadPerWorker + 1);
            }
        }
        
        try {
            System.out.println("Waiting until Producers are done... ");
            producerCountDown.await();
            System.out.println("Producers are done. Total time: " + (System.currentTimeMillis() - startTimeProducer) / 1000 + " seconds");
        } catch (InterruptedException ex) {
            Logger.getLogger(StartProducers.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            producerThreadPool.shutdown();
        }
    }
}
