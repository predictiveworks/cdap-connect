package de.kp.works.connect.snowflake;
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

import java.util.Locale;

import com.google.common.base.Strings;

import de.kp.works.connect.common.jdbc.JdbcSinkConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import javax.annotation.Nullable;

public class SnowflakeSinkConfig extends JdbcSinkConfig {

	private static final long serialVersionUID = 8103549975135456890L;

	@Description("Name of the Snowflake account.")
	@Macro
	public String account;

	@Description("The name of the cloud region where the data is geographically stored."
				+ " The region also determines where computing resources are provisioned.")
	@Macro
	public String region;

	@Description("Specifies the OAuth token to use for authentication. This parameter is"
				+ " required only when setting the authenticator parameter to 'oauth'."
			    + " This field is optional, but either an authentication token or a user"
			    + " password must be provided.")
	@Macro
	@Nullable
	public String authToken;

	@Description("Specifies the authenticator to use for verifying user login credentials."
			+ " You can set this to one of the following values: 'snowflake', 'externalbrowser',"
			+ " 'https://<okta_account_name>.okta.com', 'oauth', 'snowflake_jwt' or 'username_password_mfa'.")
	@Macro
	@Nullable
	public String authenticator;

	@Description("Name of the database to import data from.")
	@Macro
	public String database;

	@Description("Name of the default schema to use for the specified database once connected, "
			+ "or an empty string. The specified schema should be an existing schema for which "
			+ "the specified default role has privileges.")
	@Macro
	@Nullable
	public String schema;

	@Description("Specifies the default access control role to use in the Snowflake session"
				+ " initiated by the driver. The specified role should be an existing role that"
			    + " has already been assigned to the specified user for the driver. If the specified"
				+ " role has not already been assigned to the user, the role is not used when the session"
				+ " is initiated by the driver.")
	@Macro
	@Nullable
	public String role;

	@Description("Name of the Snowflake data warehouse to import data from.")
	@Macro
	@Nullable
	public String warehouse;

	@Description("Indicator to determine whether a Snowflake connection must use SSL. Values are"
				+ " 'on' or 'off. Default value = 'on'.")
	@Macro
	@Nullable
	public String ssl;

	public String getAuthenticator() {
		if (Strings.isNullOrEmpty(authenticator)) return "snowflake";
		return authenticator;
	}

	public String getSchema() {
		if (Strings.isNullOrEmpty(schema)) return "public";
		return schema;
	}

	public String getSsl() {
		if (Strings.isNullOrEmpty(ssl)) return "on";
		return ssl;
	}

	public void validate() {
		super.validate();
		
		if (Strings.isNullOrEmpty(user)) {
			throw new IllegalArgumentException(
					String.format("[%s] The user name must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(authToken) && Strings.isNullOrEmpty(password)) {
			throw new IllegalArgumentException(
					String.format("[%s] Either authentication token or password must not be empty.",
							this.getClass().getName()));
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
		/*
		 * The endpoint is a combination of account (name) and region
		 */
		if (region.equals("us-west-2")) {
			return String.format(Locale.ENGLISH,
					"jdbc:snowflake://%s.snowflakecomputing.com", account);
		}
		else {
			return String.format(Locale.ENGLISH,
					"jdbc:snowflake://%s.%s.snowflakecomputing.com", account, region);

		}

	}

}
