package de.kp.works.connect.jdbc.snowflake;
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

import java.util.Locale;

import com.google.common.base.Strings;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import de.kp.works.connect.jdbc.JdbcSourceConfig;

public class SnowflakeSourceConfig extends JdbcSourceConfig {

	private static final long serialVersionUID = -8763612701899671095L;

	@Description("Name of the Snowflake data warehouse to import data from.")
	@Macro
	public String warehouse;

	@Description("Name of the Jdbc database to import data from.")
	@Macro
	public String database;

	@Description("Name of the Snowflake account.")
	@Macro
	public String account;

	@Description("Name of the default schema to use for the specified database once connected, "
			+ "or an empty string. The specified schema should be an existing schema for which "
			+ "the specified default role has privileges.")
	@Macro
	public String schema;
	
	public void validate() {
		super.validate();
		
		if (Strings.isNullOrEmpty(user)) {
			throw new IllegalArgumentException(
					String.format("[%s] The user name must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(password)) {
			throw new IllegalArgumentException(
					String.format("[%s] The password must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(warehouse)) {
			throw new IllegalArgumentException(
					String.format("[%s] The warehouse name must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(database)) {
			throw new IllegalArgumentException(
					String.format("[%s] The database name must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(account)) {
			throw new IllegalArgumentException(
					String.format("[%s] The Snowflake account must not be empty.", this.getClass().getName()));
		}
		
	}

	public String getEndpoint() {
		return String.format(Locale.ENGLISH, "jdbc:snowflake://%s.snowflakecomputing.com", account);
	}

}
