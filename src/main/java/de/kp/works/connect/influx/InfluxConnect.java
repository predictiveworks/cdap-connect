package de.kp.works.connect.influx;
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import java.util.List;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

public class InfluxConnect {

	private String connection;
	private String database;

	private String user;
	private String password;

	private String measurement;
	private InfluxDB influxDB;

	public InfluxConnect() {

	}

	/*
	 * This helper method leverage InfluxDB ping to check whether the connection is
	 * valid
	 */
	public Boolean isValidConnection() {

		try {
			influxDB = InfluxDBFactory.connect(connection, user, password);

			Pong response = influxDB.ping();
			if (response.getVersion().equalsIgnoreCase("unknown")) {
				return false;
			}

			return true;

		} catch (Exception e) {
			return false;
		}
	}

	public void createDatabase(String duration, Integer replication) throws Exception {

		if (influxDB == null)
			throw new Exception("Connection must be validated before this method can be used.");

		QueryResult createDB = influxDB.query(new Query(String.format("CREATE DATABASE %s", database)));
		/*
		 * We do not expect any error at this stage; therefore the error is used to
		 * throw an exception
		 */
		if (createDB.hasError())
			throw new Exception(createDB.getError());

		String rp_stmt = String.format(
				"CREATE RETENTION POLICY %s_policy ON %s DURATION %s REPLICATION %s DEFAULT", database, database,
				duration, replication);
		QueryResult createRP = influxDB.query(new Query(rp_stmt));
		/*
		 * We do not expect any error at this stage; therefore the error is used to
		 * throw an exception
		 */
		if (createRP.hasError())
			throw new Exception(createRP.getError());

	}
	/*
	 * This is a helper method to check whether the provided database exists
	 */
	public Boolean databaseExists() throws Exception {

		if (influxDB == null)
			throw new Exception("Connection must be validated before this method can be used.");

		QueryResult queryResult = influxDB.query(new Query("SHOW DATABASES"));
		/*
		 * We do not expect any error at this stage; therefore the error is used to
		 * throw an exception
		 */
		if (queryResult.hasError())
			throw new Exception(queryResult.getError());

		Boolean exists = false;
		/*
		 * Sample of an InfluxDB response
		 * 
		 * {"results":[{"series":[{"name":"databases","columns":["name"],"values":[[
		 * "mydb"]]}]}]} Series [name=databases, columns=[name], values=[[mydb],
		 * [unittest_1433605300968]]]
		 * 
		 */
		List<List<Object>> dbs = queryResult.getResults().get(0).getSeries().get(0).getValues();

		if (dbs != null) {

			for (List<Object> db : dbs) {

				String name = db.get(0).toString();
				if (name.equals(database))
					exists = true;

			}

		}

		return exists;

	}

	public Boolean measurementExists() throws Exception {

		if (influxDB == null)
			throw new Exception("Connection must be validated before this method can be used.");

		QueryResult queryResult = influxDB.query(new Query("SHOW MEASUREMENTS", database));
		/*
		 * We do not expect any error at this stage; there the error is used to throw an
		 * exception
		 */
		if (queryResult.hasError())
			throw new Exception(queryResult.getError());

		Boolean exists = false;
		List<List<Object>> mms = queryResult.getResults().get(0).getSeries().get(0).getValues();

		if (mms != null) {

			for (List<Object> mm : mms) {

				String name = mm.get(0).toString();
				if (name.equals(measurement))
					exists = true;

			}

		}

		return exists;
	}

	public InfluxConnect setConnection(String connection) {
		this.connection = connection;
		return this;
	}

	public InfluxConnect setDatabase(String database) {
		this.database = database;
		return this;
	}

	public InfluxConnect setMeasurement(String measurement) {
		this.measurement = measurement;
		return this;
	}

	public InfluxConnect setUser(String user) {
		this.user = user;
		return this;
	}

	public InfluxConnect setPassword(String password) {
		this.password = password;
		return this;
	}

}
