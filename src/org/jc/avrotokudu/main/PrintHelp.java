/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.avrotokudu.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 *
 * @author Jorge Céspedes
 */
public class PrintHelp {
    
    public static final String HELP_FILE_PRODUCER = "help_producer.txt";
    
    public static final String HELP_FILE_CONSUMER = "help_consumer.txt";
    
    public String getHelpText(String textFile) throws IOException {
        //return new String(Files.readAllBytes(Paths.get(textFile)));
        InputStream in = getClass().getResourceAsStream("/" + textFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null)
            sb.append(line).append("\n");
        
        return sb.toString();
    }
}
