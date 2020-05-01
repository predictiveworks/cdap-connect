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

import javax.annotation.Nullable;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import de.kp.works.connect.jdbc.JdbcSource;


@Plugin(type = "batchsource")
@Name("CrateSource")
@Description("A batch source to read structured records from the Crate IoT scale database.")
public class CrateSource extends JdbcSource {
	
	private static final String JDBC_DRIVER_NAME = "io.crate.client.jdbc.CrateDriver";
	private static final String JDBC_PLUGIN_ID = "source.jdbc.crate";
	/*
	 * 'type' and 'name' must match the provided JSON specification
	 */
	private static final String JDBC_PLUGIN_TYPE = "jdbc";
	private static final String JDBC_PLUGIN_NAME = "crate";

	private final CrateSourceConfig cfg;

	public CrateSource(CrateSourceConfig crateConfig) {
		this.cfg = crateConfig;
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
		return cfg.getEndpoint();
	};

	@Override
	protected String getUser() {
		return cfg.user;
	}

	@Override
	protected String getPassword() {
		return cfg.password;
	};

	@Override
	protected String getCountQuery() {
		return cfg.getInputCountQuery();
	};

	@Override
	protected String getInputQuery() {
		return cfg.getInputQuery();
	};

	@Override
	protected String getReferenceName() {
		return cfg.referenceName;
	};
	
	@Override
	protected void validate() {
		cfg.validate();
	}

	public static class CrateSourceConfig extends CrateConfig {

		private static final long serialVersionUID = -282545235226670018L;

		/*
		 * The name of the Crate database table to read data from.
		 */
		public static final String TABLE_NAME = "tableName";
		/*
		 * The input query defines the custom query to read data from a Crate database;
		 * the input query must be provided by the user
		 */
		public static final String INPUT_QUERY = "inputQuery";
		/*
		 * The CrateSource is made for users with restricted data engineering skills and
		 * therefore does not support the following more advanced paramaters that are
		 * useful in a big data environment:
		 * 
		 * bounding query
		 * 
		 * The bounding query is used to determine the amount of data and to separate
		 * them into roughly equivalent shards.
		 * 
		 * split by field
		 * 
		 * The split by field represents a numeric column that is used to separate the
		 * rows into equivalent shards.
		 * 
		 * 
		 */
		@Description("Name of the Crate table to import data from.")
		@Nullable
		@Macro
		public String tableName;

		@Name(INPUT_QUERY)
		@Description("The SQL select statement to import data from the Crate database. "
				+ "For example: select * from <your table name>'.")
		@Nullable
		@Macro
		public String inputQuery;

		public CrateSourceConfig() {
			super();
		}

		public String getInputCountQuery() {
			
			String countQuery = String.format("select count(*) from (%s) as crate", getInputQuery());
			return countQuery;

		}
		
		public String getInputQuery() {

			if (Strings.isNullOrEmpty(inputQuery))
				return String.format("select * from %s", tableName);

			return inputQuery;

		}

		public void validate() {

			if (Strings.isNullOrEmpty(tableName) && Strings.isNullOrEmpty(inputQuery))
				throw new IllegalArgumentException(
						"Either table name or query can be empty, however both parameters are empty. "
								+ "This connector is not able to import data from the Crate database.");

		}

	}

}
