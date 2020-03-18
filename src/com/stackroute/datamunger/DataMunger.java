package com.stackroute.datamunger;

import java.util.HashMap;

import com.stackroute.datamunger.query.Query;
import com.stackroute.datamunger.writer.JsonWriter;

public class DataMunger {

	public static void main(String[] args) {
		
		 Query queryObj = null;
		JsonWriter jsonWriterObj = null;

		// Read the query from the user
		String queryString = "select city, win_by_runs, season from data/ipl.csv where season > 2014 and city ='Bangalore'";
		/*
		 * Instantiate Query class. This class is responsible for: 1. Parsing the query
		 * 2. Select the appropriate type of query processor 3. Get the resultSet which
		 * is populated by the Query Processor
		 */

		queryObj = new Query();
		/*
		 * Instantiate JsonWriter class. This class is responsible for writing the
		 * ResultSet into a JSON file
		 */
		jsonWriterObj = new JsonWriter();
		/*
		 * call executeQuery() method of Query class to get the resultSet. Pass this
		 * resultSet as parameter to writeToJson() method of JsonWriter class to write
		 * the resultSet into a JSON file
		 */
		@SuppressWarnings("rawtypes")
		HashMap resultSet = queryObj.executeQuery(queryString);

		Boolean bool = jsonWriterObj.writeToJson(resultSet);
		if (bool) {
			System.out.println("Success");
		} else {
			System.out.println("Failure");
		}


	}
}
