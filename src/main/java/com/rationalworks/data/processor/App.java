package com.rationalworks.data.processor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.simple.parser.ParseException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws FileNotFoundException, IOException, ParseException
    {
    	
        System.out.println( "Begin");
        
        //DataProcessorEngine.initilize();
        DataProcessorEngine.initilize(8082);
        
        //Load all transformations
        String folderWithStoreDefinitions ="src/test/resources";
        DataProcessorEngine.loadXmls(folderWithStoreDefinitions);
      

        ExecutorService pool = Executors.newFixedThreadPool(5);
        for(int i=0;i<10000;i++)
        {
        	 Runnable r1 = new DataProcessJob();
        	 pool.execute(r1);
        }
         
        pool.shutdown();
    }
}
