package de.kp.works.connect.panoply;
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

import java.sql.Driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import de.kp.works.connect.jdbc.JdbcSource;
import de.kp.works.connect.redshift.RedshiftSource;
import de.kp.works.connect.redshift.RedshiftSourceConfig;

@Plugin(type = "batchsource")
@Name("PanoplySource")
@Description("A batch source to read structured records from a Panoply data warehouse.")
public class PanoplySource extends JdbcSource {

	protected static final Logger LOG = LoggerFactory.getLogger(RedshiftSource.class);
	
	protected static final String JDBC_DRIVER_NAME = "com.amazon.redshift.jdbc42.Driver";

	protected static final String JDBC_PLUGIN_ID = "source.jdbc.redshift";
	protected Class<? extends Driver> driverClass;
	
	/*
	 * 'type' and 'name' must match the provided JSON specification
	 */
	protected static final String JDBC_PLUGIN_TYPE = "jdbc";
	protected static final String JDBC_PLUGIN_NAME = "redshift";

	protected RedshiftSourceConfig config;
	
	public PanoplySource(RedshiftSourceConfig config) {
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
	};

	@Override
	protected String getUser() {
		return config.user;
	}

	@Override
	protected String getPassword() {
		return config.password;
	};

	@Override
	protected String getCountQuery() {
		return config.getCountQuery();
	};

	@Override
	protected String getInputQuery() {
		return config.getInputQuery();
	};

	@Override
	protected String getReferenceName() {
		return config.referenceName;
	};
	
	@Override
	protected void validate() {
		config.validate();
	}

}
