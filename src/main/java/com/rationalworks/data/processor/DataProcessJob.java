package com.rationalworks.data.processor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class DataProcessJob implements Runnable {

	private static final MetricRegistry metrics = new MetricRegistry();
	private static final Meter submissionRate = metrics.meter("submission");
	private static final Meter completionRate = metrics.meter("completion");
	
	public void run() {
		DataProcessEngineSession dpsession = DataProcessorEngine.getSession();
		String inputDataFile = "src/test/resources/input-employment.json";
		JSONParser parser = new JSONParser();
		JSONObject inputJson;
		try {
			inputJson = (JSONObject) parser.parse(new FileReader(new File(inputDataFile)));
			submissionRate.mark();
			System.out.println("Requests submitted per minute: "+ submissionRate.getOneMinuteRate());
			dpsession.loadJsonData(inputJson, "employment");
			JSONObject opJson = dpsession.fetchData("employmentperyearwithmean");
			dpsession.closeSession();
			completionRate.mark();
			System.out.println("Requests processed per minute: "+ completionRate.getOneMinuteRate());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
