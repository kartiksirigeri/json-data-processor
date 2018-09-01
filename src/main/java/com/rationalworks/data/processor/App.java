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
        
        String folderWithStoreDefinitions ="src/test/resources";
        DataProcessorEngine.loadXmls(folderWithStoreDefinitions);
          
        String inputDataFile ="src/test/resources/input-cars.json";
        DataProcessorEngine.loadJsonData(inputDataFile,"cars");
        
        String employmentDataFile ="src/test/resources/input-employment.json";
        DataProcessorEngine.loadJsonData(employmentDataFile,"employment");
        
        System.out.println("Min of age by name");
        DataProcessorEngine.loadOutputData("employmentperyear");
        
        System.out.println( "End" );
        //DataProcessorEngine.shutdown();
         
         
    }
}
