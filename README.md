# json-data-processor

# Project Title

JSON data processor was an attempt tried over weekend to understand the challenges of transforming json data with ease.
This is not a production ready library, but it surely does bare minimum.

The idea is to use in-memory database H2 and apply the data transformation using external xmls.

The way it's achieved is as following.
When the DataProcessorEngine starts, it reads all xmls from location specified.
The xml are translated to SQL DDL queries and executed on inmemory H2 database.
The above steps creates tables as well as views.
The input data is inserted next.
Since the transformations are defined as views, the transformed values can now be queried by using DataProcessorEngine.loadOutputData() api.

## Getting Started

It's simple.

```
git clone git@github.com:rationalworks/json-data-processor.git
```
Open it with your favourite IDE.

Run the App.java class from com.rationalworks.data.processor package.

Important files worth looking
```
src/test/resources/input-cars.json
src/test/resources/input-employment.json
```

```
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
DataProcessorEngine.loadOutputData("meanemploymentratebyseries")
```

You can also check your data by navigating to the following URL
```
http://localhost:8082
```
JDBC URL:jdbc:h2:mem:storage
User name: sa
Password: sa

### Prerequisites

Setup JDK

Setup Maven

### Installing

Use the jar generated under target folder.


## Deployment

I suggest not to deploy in production, you are always free to fork and make changes as per your requirements.

## Built With

* [H2](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management


## Authors

* **Praveen** - *praveen.sirt@gmail.com* - [Profile](https://github.com/praveen2k10)


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* H2 team

