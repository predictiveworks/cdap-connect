package de.kp.works.connect.influx;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import javax.annotation.Nullable;

import com.google.common.base.Strings;

import de.kp.works.connect.common.BaseConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

public class InfluxConfig extends BaseConfig {

	private static final long serialVersionUID = -158092676227470967L;

	/*** CONNECTION PARAMETERS ***/
	
	@Description("The host of the database.")
	@Macro
	public String host;

	@Description("The port of the database.")
	@Macro
	public String port;

	@Description("The protocol of the database connection. Supports values are 'http' and 'https'.")
	@Macro
	public String protocol;

	@Description("The name of an existing InfluxDB database, used to write time points to.")
	@Macro
	public String database;

	@Description("The name of the measurements used to write time points to.")
	@Macro
	public String measurement;
	
	@Description("The retention interval used when the database does not exist. Default is '30d'.")
	@Macro
	public String duration;
	
	@Description("The replication factor used when the database does not exist. Default is 1.")
	@Macro
	public Integer replication;

	@Description("The optional name of the field in the input schema that contains the timestamp in milliseconds.")
	@Macro
	@Nullable
	public String timefield;

	/*** CREDENTIALS ***/
	
	@Description("Name of a registered user name. Required for authentication.")
	@Macro
	public String user;

	@Description("Password of the registered user. Required for authentication.")
	public String password;
	
	public InfluxConfig() {
		duration = "30d";
		replication = 1;
	}
	
	public String getConnectionString() {
		return String.format("%s://%s:%s", protocol, host, port);
	}
	
	public void validate() {
		super.validate();
		
		if (Strings.isNullOrEmpty(host)) {
			throw new IllegalArgumentException(
					String.format("[%s] The database host must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(port)) {
			throw new IllegalArgumentException(
					String.format("[%s] The database port must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(protocol)) {
			throw new IllegalArgumentException(
					String.format("[%s] The database connection protocol must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(database)) {
			throw new IllegalArgumentException(
					String.format("[%s] The database name must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(measurement)) {
			throw new IllegalArgumentException(
					String.format("[%s] The measurement must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(user)) {
			throw new IllegalArgumentException(
					String.format("[%s] The user name must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(password)) {
			throw new IllegalArgumentException(
					String.format("[%s] The user password must not be empty.", this.getClass().getName()));
		}
		
	}

}
