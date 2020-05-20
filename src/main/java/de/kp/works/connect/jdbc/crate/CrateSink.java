package de.kp.works.connect.jdbc.crate;
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
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import de.kp.works.connect.jdbc.JdbcSink;

/*
 * __KUP__
 * 
 * This Crate sink connector does not require the previous
 * stages to provide an input schema; if there is no schema
 * available this sink leverages the record schema from the
 * first record and creates a new table with schema compliant
 * fields on the fly.
 */

@Plugin(type = "batchsink")
@Name("CrateSink")
@Description("A batch sink to write structured records to Crate IoT scale database.")
public class CrateSink extends JdbcSink<CrateWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(CrateSink.class);

	private static final String JDBC_DRIVER_NAME = "io.crate.client.jdbc.CrateDriver";
	private static final String JDBC_PLUGIN_ID = "sink.jdbc.crate";
	/*
	 * 'type' and 'name' must match the provided JSON specification
	 */
	private static final String JDBC_PLUGIN_TYPE = "jdbc";
	private static final String JDBC_PLUGIN_NAME = "crate";

	private CrateSinkConfig cfg;
	private CrateConnect connect;

	public CrateSink(CrateSinkConfig crateConfig) {

		this.cfg = crateConfig;
		this.connect = new CrateConnect(cfg.getEndpoint(), cfg.tableName, cfg.primaryKey);

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
	protected Properties getProperties() {

		Properties properties = new Properties();

		if (cfg.user == null || cfg.password == null)
			return properties;

		properties.put("user", cfg.user);
		properties.put("password", cfg.password);

		return properties;
	};

	@Override
	protected String getTableName() {
		return cfg.tableName;
	};

	protected String getPrimaryKey() {
		return cfg.primaryKey;
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
		context.addOutput(Output.of(cfg.referenceName, new CrateOutputFormatProvider(prepareConf(schema))));

	}

	private Map<String, String> prepareConf(Schema schema) {

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
		 * 
		 * The primary key of the table is important, as this [CrateSink] supports
		 * JDBC's DUPLICATE ON KEY feature to enable proper update requests in case of
		 * key conflicts.
		 * 
		 * The property 'mapreduce.jdbc.primaryKey' is an internal provided property
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
	public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, CrateWritable>> emitter)
			throws Exception {
		emitter.emit(new KeyValue<NullWritable, CrateWritable>(null, new CrateWritable(connect, input)));
	}

	/**
	 * The [CrateOutputFormatProvider] supports use cases where the schema is
	 * specified by the previous stage and those, where the schema is not available
	 * (implicit schema derivation)
	 */
	private static class CrateOutputFormatProvider implements OutputFormatProvider {

		private final Map<String, String> conf;

		CrateOutputFormatProvider(Map<String, String> conf) {
			this.conf = conf;
		}

		/**
		 * The following method connect the output format provider with a specific
		 * output format and the dynamically delivered configuration
		 */
		@Override
		public String getOutputFormatClassName() {
			return CrateOutputFormat.class.getName();
		}

		@Override
		public Map<String, String> getOutputFormatConfiguration() {
			return conf;
		}

	}
}
