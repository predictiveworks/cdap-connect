package de.kp.works.connect.saphana;
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

import java.util.Properties;

import de.kp.works.connect.common.jdbc.JdbcSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;

@Plugin(type = "batchsource")
@Name("SAPHanaSource")
@Description("A Works batch connector for reading structured records from an SAP HANA database," +
		"and transforming them into structured data flow records.")
public class SAPHanaSource extends JdbcSource {

	protected static final Logger LOG = LoggerFactory.getLogger(SAPHanaSource.class);
	
	protected static final String JDBC_DRIVER_NAME = "com.sap.db.jdbc.Driver";
	protected static final String JDBC_PLUGIN_ID = "source.jdbc.saphana";
	
	/*
	 * 'type' and 'name' must match the provided JSON specification
	 */
	protected static final String JDBC_PLUGIN_TYPE = "jdbc";
	protected static final String JDBC_PLUGIN_NAME = "saphana";

	protected SAPHanaSourceConfig config;
	
	public SAPHanaSource(SAPHanaSourceConfig config) {
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
		
		if (config.user == null || config.password == null)
			return properties;
		
		properties.put("user", config.user);
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
