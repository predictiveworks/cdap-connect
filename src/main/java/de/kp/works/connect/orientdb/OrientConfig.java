package de.kp.works.connect.orientdb;
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

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import de.kp.works.connect.BaseConfig;

public class OrientConfig extends BaseConfig {

	private static final long serialVersionUID = 2202924961246568490L;

	/*** CONNECTION PARAMETERS ***/
	
	@Description("The host (IP address) of the OrientDB.")
	@Macro
	public String host;

	@Description("The name of the OrientDB database.")
	@Macro
	public String database;

	/*** CREDENTIALS ***/
	
	@Description("Name of a registered user name. Required for authentication.")
	@Macro
	public String user;

	@Description("Password of the registered user. Required for authentication.")
	public String password;

	public void validate() {
		super.validate();
		
		/*** CONNECTION PARAMETERS ***/
		
		if (Strings.isNullOrEmpty(host)) {
			throw new IllegalArgumentException(
					String.format("[%s] The database host must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(database)) {
			throw new IllegalArgumentException(
					String.format("[%s] The database name must not be empty.", this.getClass().getName()));
		}

		/*** CREDENTIALS ***/
		
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
