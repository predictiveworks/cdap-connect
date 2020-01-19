package de.kp.works.connect.crate;
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;

import java.util.Locale;

import javax.annotation.Nullable;

public class ConnectionConfig extends PluginConfig {
	/*
	 * This is a base configuration class; validation of the
	 * provided parameters is performed in the derived source
	 * and sink configuration
	 */
	private static final long serialVersionUID = -4210860159645495005L;

	public static final String HOST = "host";
	public static final String PORT = "port";

	public static final String USER = "user";
	public static final String PASSWORD = "password";

	@Name(HOST)
	@Description("The host of the Crate database.")
	@Macro
	public String host;

	@Name(PORT)
	@Description("The port of the Crate database.")
	@Macro
	public String port;

	@Name(USER)
	@Description("User name. Required for databases that need authentication. Optional for databases that do not require authentication.")
	@Nullable
	@Macro
	public String user;

	@Name(PASSWORD)
	@Description("Password. Required for databases that need authentication. Optional for databases that do not require authentication.")
	@Nullable
	@Macro
	public String password;
	
	public ConnectionConfig(String host, String port, @Nullable String user, @Nullable String password) {

		this.host = host;
		this.port = port;

		this.user = user;
		this.password = password;

	}

	public String getConnectionString() {
		return String.format(Locale.ENGLISH, "jdbc:crate://%s:%s/", host, port);
	}
	
	public String getDriverName() {
		return "io.crate.client.jdbc.CrateDriver";
	}
	
	public Boolean getEnableAutoCommit() {
		return false;
	}

}
