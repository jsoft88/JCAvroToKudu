/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.avrotokudu.producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.jc.avrotokudu.main.StartProducers;
import org.jc.kafka.producer.GenericProducer;

/**
 *
 * @author Jorge Céspedes
 * This is a delimited avro reader because each file has a number of workers
 * reading chunks of this file. The number of readers and the number of workers
 * per file can be set through command line arguments.
 */
public class DelimitedAvroReaderWorker extends GenericProducer<String, String, String, String> 
                            implements Runnable {
    
    public static final int MAX_FETCH_SIZE = 100;

    private final String topic;
    
    private final int partitionToProduceFor;
    
    private final long startPositionInFile;
    
    private final long endPositionInFile;
    
    private final String fileInHdfs;
    
    private final String keyToUseForPartition;
    
    private FileReader<Object> fileReader;
    
    public DelimitedAvroReaderWorker(String topic,
                                    int partitionToProduceFor, 
                                    long startPositionInFile,
                                    long endPositionInFile,
                                    String fileInHdfs,
                                    String keyToUseForPartition,
                                    ProducerConfig config){
        super(config);
        this.partitionToProduceFor = partitionToProduceFor;
        this.startPositionInFile = startPositionInFile;
        this.endPositionInFile = endPositionInFile;
        this.fileInHdfs = fileInHdfs;
        this.keyToUseForPartition = keyToUseForPartition;
        this.topic = topic;
    }
    
    @Override
    public void sendMessageBatch(List<KeyedMessage<String, String>> messages) {
        super.sendMessageBatch(messages); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void sendMessage(KeyedMessage<String, String> message) {
        super.sendMessage(message); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMessageKey(String haystack) {
        int posOfKey = haystack.indexOf(this.keyToUseForPartition);
        //Default is partition 0.
        if (posOfKey < 0) return "0";
        int posOfColon = haystack.indexOf(":", posOfKey);
        String partitionKey = haystack.substring(posOfColon + 2, haystack.indexOf(",", posOfColon));
        
        return partitionKey.trim();
    }
    
    @Override
    public void run() {
        this.produce();
    }

    @Override
    public void produce(){
        try {
            Path path = new Path(this.fileInHdfs);
            FsInput input = new FsInput(path, new Configuration());
            GenericDatumReader<Object> reader = new GenericDatumReader<>();
            this.fileReader = DataFileReader.openReader(input, reader);
            
            this.fileReader.sync(this.startPositionInFile);
            int count = 0;
            List<KeyedMessage<String, String>> messages = new ArrayList<>();
            while (this.fileReader.hasNext() && !this.fileReader.pastSync(this.endPositionInFile)) {
                Object datum = this.fileReader.next();
                
                String stringifiedMessage = datum.toString();
                System.out.println(stringifiedMessage);
                KeyedMessage<String, String> data = 
                                        new KeyedMessage<>(this.topic, this.getMessageKey(stringifiedMessage), stringifiedMessage);
                messages.add(data);
                
                if ( (count = (count + 1) % MAX_FETCH_SIZE) == 0 ) {
                    //send message now
                    System.out.println(
                            "Now sending messages batch to kafka: " + messages.size() + " messages");
                    
                    this.sendMessageBatch(messages);
                    messages.clear();
                }
            }
            
            //Send what's left in message list.
            if (count > 0) this.sendMessageBatch(messages);
        } catch (IOException ex) {
            Logger.getLogger(DelimitedAvroReaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            StartProducers.producerCountDown.countDown();
        }
    }
    
}
