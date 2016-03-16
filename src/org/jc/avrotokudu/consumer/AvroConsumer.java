/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.avrotokudu.consumer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.LongSerializationPolicy;
import com.google.gson.reflect.TypeToken;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.message.MessageAndOffset;
import org.jc.kafka.consumer.GenericConsumer;
import org.jc.kududbhelper.lib.KuduHelper;
import org.jc.kududbhelper.lib.KuduHelperFactory;
import org.kududb.client.SessionConfiguration;

/**
 *
 * @author cespedjo
 */
public class AvroConsumer extends GenericConsumer<String, String> {
    
    private Gson gson;
    
    private static final Type stringStringMap = 
            new TypeToken<HashMap<String, String>>(){}.getType();
    
    private static KuduHelper kuduHelper;

    public AvroConsumer(String consumerGroup, 
            String topic, 
            int partitionToConsume, 
            String[] seedBrokers, 
            int soTimeout, 
            int port, 
            int maxEmptyPartitionReadTries,
            String tableName,
            String[] masterAddresses) {
        super(consumerGroup, topic, partitionToConsume, seedBrokers, soTimeout, port, maxEmptyPartitionReadTries);
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.setLongSerializationPolicy(LongSerializationPolicy.STRING);
        
        this.gson = gsonBuilder.create();
        try {
            AvroConsumer.kuduHelper = KuduHelperFactory
                    .buildKuduHelper(tableName, masterAddresses);
            AvroConsumer.kuduHelper.init(KuduHelper.KUDU_SESSION_ASYNC, 
                    SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        } catch (Exception ex) {
            Logger.getLogger(AvroConsumer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void processConsumedMessage(MessageAndOffset messageAndOffset) {
        ByteBuffer payload = messageAndOffset.message().payload();

        byte[] bytes = new byte[payload.limit()];
        payload.get(bytes);
        try {
            String json = new String(bytes, "UTF-8");
            Map<String, String> aRow = this.gson.fromJson(json, AvroConsumer.stringStringMap);

            try {
                AvroConsumer.kuduHelper.processMap(Arrays.asList(aRow));
            } catch (Exception ex) {
                Logger.getLogger(AvroConsumer.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (UnsupportedEncodingException | JsonSyntaxException ex) {
            Logger.getLogger(AvroConsumer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
