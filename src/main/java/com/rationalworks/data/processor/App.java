package com.rationalworks.data.processor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
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
        
        DataProcessorEngine.initilize();
        //DataProcessorEngine.initilize(8082);
        
        //Load all transformations
        String folderWithStoreDefinitions ="src/test/resources";
        DataProcessorEngine.loadXmls(folderWithStoreDefinitions);
     
        DataProcessEngineSession dpsession = DataProcessorEngine.getSession();
        String inputDataFile ="src/test/resources/input-employment.json";
        JSONParser parser = new JSONParser();
        JSONObject inputJson = (JSONObject) parser.parse(new FileReader(new File(inputDataFile)));
        dpsession.loadJsonData(inputJson, "employment");
        
        JSONObject opJson = dpsession.fetchData("meanemploymentrate");
        System.out.println(opJson.toJSONString());
         
    }
}
