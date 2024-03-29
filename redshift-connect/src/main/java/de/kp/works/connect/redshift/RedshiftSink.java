package de.kp.works.connect.redshift;
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

import java.sql.Driver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import de.kp.works.connect.common.jdbc.JdbcSink;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Joiner;
import com.google.gson.Gson;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;

@Plugin(type = "batchsink")
@Name("RedshiftSink")
@Description("A Works batch connector for writing structured records to a Redshift data warehouse database. "
		+ "It is not recommended to use this sink connector for (very) large datasets. In this case "
		+ "leverage the S3 sink connector and the S3-to-Redshift action.")
public class RedshiftSink extends JdbcSink<RedshiftWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(RedshiftSink.class);
	
	protected static final String JDBC_DRIVER_NAME = "com.amazon.redshift.jdbc42.Driver";

	protected static final String JDBC_PLUGIN_ID = "sink.jdbc.redshift";
	protected Class<? extends Driver> driverClass;
	
	/*
	 * 'type' and 'name' must match the provided JSON specification
	 */
	protected static final String JDBC_PLUGIN_TYPE = "jdbc";
	protected static final String JDBC_PLUGIN_NAME = "redshift";

	private final RedshiftSinkConfig config;
	private final RedshiftConnect connect;

	public RedshiftSink(RedshiftSinkConfig config) {
		this.config = config;
		this.connect = new RedshiftConnect(config);
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
	protected String getTableName() {
		return config.tableName;
	}
	
	protected String getPrimaryKey() {
		return config.primaryKey;
	}
	
	private Map<String,String> prepareConf(Schema schema) {

		Map<String, String> conf = new HashMap<>();
		/*
		 * CONNECTION PROPERTIES
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

			assert schema.getFields() != null;
			for (Schema.Field field : schema.getFields()) {
				fieldNames.add(field.getName());
			}

			conf.put(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, Joiner.on(",").join(fieldNames));

		}
		
		return conf;
	
	}

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
		context.addOutput(Output.of(config.referenceName, new RedshiftOutputFormatProvider(prepareConf(schema))));

	}
	
	@Override
	public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, RedshiftWritable>> emitter) {
		emitter.emit(new KeyValue<>(null, new RedshiftWritable(connect, input)));
	}

	/**
	 * The [RedshiftOutputFormatProvider] supports use cases where the schema is
	 * specified by the previous stage and those, where the schema is not available
	 * (implicit schema derivation)
	 */
	private static class RedshiftOutputFormatProvider implements OutputFormatProvider {

		private final Map<String, String> conf;

		RedshiftOutputFormatProvider(Map<String, String> conf) {
			this.conf = conf;
		}

		/**
		 * The following method connect the output format provider with a specific
		 * output format and the dynamically delivered configuration
		 */
		@Override
		public String getOutputFormatClassName() {
			return RedshiftOutputFormat.class.getName();
		}

		@Override
		public Map<String, String> getOutputFormatConfiguration() {
			return conf;
		}

	}

}
