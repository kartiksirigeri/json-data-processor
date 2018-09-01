package com.rationalworks.data.processor;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	
        System.out.println( "Begin");
        DataProcessorEngine.initilize();
        
        //Load all transformations
        String folderWithStoreDefinitions ="src/test/resources";
        DataProcessorEngine.loadXmls(folderWithStoreDefinitions);
          
        //Load input data set
        String inputDataFile ="src/test/resources/input-cars.json";
        DataProcessorEngine.loadJsonData(inputDataFile,"cars");
        
        //Load another input data set
        String employmentDataFile ="src/test/resources/input-employment.json";
        DataProcessorEngine.loadJsonData(employmentDataFile,"employment");
        
        //Extract output data, note that multiple data set can be queries by 
        //repeating the below statement for different store names
        System.out.println("Mean employment by year");
        DataProcessorEngine.loadOutputData("meanemploymentratebyseries");
        
        System.out.println( "End" );
        //DataProcessorEngine.shutdown();
         
         
    }
}
