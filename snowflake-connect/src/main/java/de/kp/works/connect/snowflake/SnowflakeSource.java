package de.kp.works.connect.snowflake;
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

import java.util.Properties;

import de.kp.works.connect.common.jdbc.JdbcSource;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.BatchSource;
import joptsimple.internal.Strings;

@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("SnowflakeSource")
@Description("A Works batch connector for reading structured records from a Snowflake data warehouse," +
			" and transforming them into structured data flow records.")
public class SnowflakeSource extends JdbcSource {

	protected static final String JDBC_DRIVER_NAME = "com.snowflake.client.jdbc.SnowflakeDriver";
	protected static final String JDBC_PLUGIN_ID = "source.jdbc.snowflake";
	
	/*
	 * 'type' and 'name' must match the provided JSON specification
	 */
	protected static final String JDBC_PLUGIN_TYPE = "jdbc";
	protected static final String JDBC_PLUGIN_NAME = "snowflake";

	protected SnowflakeSourceConfig config;
	
	public SnowflakeSource(SnowflakeSourceConfig config) {
		this.config = config;
	}

	@Override
	protected String getJdbcPluginID() {
		return JDBC_PLUGIN_ID;
	}

	@Override
	protected String getJdbcPluginName() {
		return JDBC_PLUGIN_NAME;
	}

	@Override
	protected String getJdbcPluginType() {
		return JDBC_PLUGIN_TYPE;
	}
	
	@Override
	protected String getJdbcDriverName() {
		return JDBC_DRIVER_NAME;
	}
	
	@Override
	protected String getEndpoint() {
		return config.getEndpoint();
	}

	@Override
	protected Properties getProperties() {

		Properties properties = new Properties();
		/*
		 * Mandatory parameters
		 *
		 * __HINT__ Providing client info is recommended, but not
		 * mandatory; the current implementation does not support
		 * client info.
		 */
		properties.put("account", config.account);
		properties.put("db", config.database);

		properties.put("schema", config.getSchema());
		properties.put("user", config.user);

		/* Always set CLIENT_SESSION_KEEP_ALIVE */
		properties.put("client_session_keep_alive", "true");

		/* Force DECIMAL for NUMBER (SNOW-33227) */
		properties.put("JDBC_TREAT_DECIMAL_AS_INT", "false");

		/* Authentication
		 *
		 * The first choice is token authentication, as the current
		 * version does not support private keys
		 */
		if (!Strings.isNullOrEmpty(config.authToken))
			properties.put("token", config.authToken);

		else
			properties.put("password", config.password);

		properties.put("authenticator", config.getAuthenticator());
		properties.put("ssl", config.getSsl());

		/* Optional parameters */

		if (!Strings.isNullOrEmpty(config.role))
			properties.put("role", config.role);

		if (!Strings.isNullOrEmpty(config.warehouse))
			properties.put("warehouse", config.warehouse);

		properties.put("password", config.password);

		return properties;

	}

	@Override
	protected String getCountQuery() {
		return config.getCountQuery();
	}

	@Override
	protected String getInputQuery() {
		return config.getInputQuery();
	}

	@Override
	protected String getReferenceName() {
		return config.referenceName;
	}
	
	@Override
	protected void validate() {
		config.validate();
	}

}
