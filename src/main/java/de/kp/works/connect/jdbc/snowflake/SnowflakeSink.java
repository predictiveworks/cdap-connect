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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Joiner;
import com.google.gson.Gson;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import de.kp.works.connect.jdbc.JdbcSink;
import joptsimple.internal.Strings;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("SnowflakeSink")
@Description("A batch sink to write structured records to a Snowflake database.")
public class SnowflakeSink extends JdbcSink<SnowflakeWritable> {

	// TODO SSL

	private static final Logger LOG = LoggerFactory.getLogger(SnowflakeSink.class);
	
	protected static final String JDBC_DRIVER_NAME = "com.snowflake.client.jdbc.SnowflakeDriver";
	protected static final String JDBC_PLUGIN_ID = "sink.jdbc.snowflake";
	
	/*
	 * 'type' and 'name' must match the provided JSON specification
	 */
	protected static final String JDBC_PLUGIN_TYPE = "jdbc";
	protected static final String JDBC_PLUGIN_NAME = "snowflake";

	private SnowflakeConnect connect;
	private SnowflakeSinkConfig config;
	
	public SnowflakeSink(SnowflakeSinkConfig config) {
		this.config = config;
		this.connect = new SnowflakeConnect(config.getEndpoint(), config.tableName, config.primaryKey);
		
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
		
		properties.put("user", config.user);
		properties.put("password", config.password);
		
		properties.put("account", config.account);
		properties.put("db", config.database);

		properties.put("warehouse", config.warehouse);
		
		if (Strings.isNullOrEmpty(config.schema))
			 properties.put("schema", "");
		
		else
			properties.put("schema", config.schema);
		
		return properties;
		
	}

	@Override
	protected String getTableName() {
		return config.tableName;
	}
	
	protected String getPrimaryKey() {
		return config.primaryKey;
	};

	@Override
	public void prepareRun(BatchSinkContext context) throws Exception {
		
		registerJdbcDriver(context);

		Schema schema = getSchema(context);
		LOG.debug("Input schema defined. Schema = {}", schema);
		/*
		 * The output format provider supports use cases where an external schema is
		 * provided and those, where schema information has to be extracted from the
		 * structured records
		 */
		context.addOutput(Output.of(config.referenceName, new SnowflakeOutputFormatProvider(prepareConf(schema))));
		
	}	
	
	private Map<String,String> prepareConf(Schema schema) {

		Map<String, String> conf = new HashMap<>();
		/*
		 * CONECTION PROPERTIES
		 */
		conf.put(DBConfiguration.DRIVER_CLASS_PROPERTY, getJdbcDriverName());
		conf.put(DBConfiguration.URL_PROPERTY, getEndpoint());
		
		String properties = new Gson().toJson(getProperties());
		conf.put("jdbc.properties", properties);

		/*
		 * TABLE PROPERTIES
		 */
		conf.put("mapreduce.jdbc.primaryKey", getPrimaryKey());
		conf.put(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, getTableName());

		if (schema != null) {

			List<String> fieldNames = Lists.newArrayList();

			for (Schema.Field field : schema.getFields()) {
				fieldNames.add(field.getName());
			}

			conf.put(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, Joiner.on(",").join(fieldNames));

		}
		
		return conf;
		
	}
	
	@Override
	public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, SnowflakeWritable>> emitter) throws Exception {
		emitter.emit(new KeyValue<NullWritable, SnowflakeWritable>(null, new SnowflakeWritable(connect, input)));
	}

	/**
	 * The [SnowflakeOutputFormatProvider] supports use cases where the schema is
	 * specified by the previous stage and those, where the schema is not available
	 * (implicit schema derivation)
	 */
	private static class SnowflakeOutputFormatProvider implements OutputFormatProvider {

		private final Map<String, String> conf;

		SnowflakeOutputFormatProvider(Map<String, String> conf) {
			this.conf = conf;
		}

		/**
		 * The following method connect the output format provider with a specific
		 * output format and the dynamically delivered configuration
		 */
		@Override
		public String getOutputFormatClassName() {
			return SnowflakeOutputFormat.class.getName();
		}

		@Override
		public Map<String, String> getOutputFormatConfiguration() {
			return conf;
		}

	}

}
