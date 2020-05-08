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

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import de.kp.works.connect.jdbc.JdbcConfig;
import de.kp.works.connect.jdbc.JdbcDriverShim;
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

	private final CrateSinkConfig cfg;
	private final CrateConnect connect;

	public CrateSink(CrateSinkConfig crateConfig) {

		this.cfg = crateConfig;
		this.connect = new CrateConnect().setHost(cfg.host).setPort(cfg.port).setUser(cfg.user)
				.setPassword(cfg.password).setTableName(cfg.tableName).setPrimaryKey(cfg.primaryKey);

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
		return cfg.getProps();
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

		LOG.debug("tableName = {}; primaryKey = {}; connectionString = {}", cfg.tableName, cfg.primaryKey,
				cfg.getEndpoint());

		Class<? extends Driver> driverClass = context.loadPluginClass(JDBC_PLUGIN_ID);

		JdbcDriverShim driverShim = new JdbcDriverShim(driverClass.newInstance());
		DriverManager.registerDriver(driverShim);

		/* Create (if not exists) database table */
		Schema schema = context.getInputSchema();
		if (schema != null) {

			LOG.debug("Input schema defined. Schema = {}", schema);

			List<String> columns = CrateUtils.getColumns(schema);

			if (connect.createTable(columns)) {
				connect.loadColumnTypes();
			}

		}
		
		/*
		 * The output format provider supports use cases where an external schema is
		 * provided and those, where schema information has to be extracted from the
		 * structured records
		 */
		context.addOutput(Output.of(cfg.referenceName, new CrateOutputFormatProvider(prepareConf(schema))));

	}

	/*
	 * @Override
  public void prepareRun(BatchSinkContext context) {

    outputSchema = context.getInputSchema();

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    // make sure that the destination table exists and column types are correct
    try {
      if (Objects.nonNull(outputSchema)) {
        validateSchema(driverClass, dbSinkConfig.tableName, outputSchema);
      } else {
        outputSchema = inferSchema(driverClass);
      }
    } finally {
      DBUtils.cleanup(driverClass);
    }

    setColumnsInfo(outputSchema.getFields());

    emitLineage(context, outputSchema.getFields());

    ConnectionConfigAccessor configAccessor = new ConnectionConfigAccessor();
    configAccessor.setConnectionArguments(dbSinkConfig.getConnectionArguments());
    configAccessor.setInitQueries(dbSinkConfig.getInitQueries());
    configAccessor.getConfiguration().set(DBConfiguration.DRIVER_CLASS_PROPERTY, driverClass.getName());
    configAccessor.getConfiguration().set(DBConfiguration.URL_PROPERTY, connectionString);
    configAccessor.getConfiguration().set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY,
                                          dbSinkConfig.getEscapedTableName());
    configAccessor.getConfiguration().set(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, dbColumns);
    if (dbSinkConfig.user != null) {
      configAccessor.getConfiguration().set(DBConfiguration.USERNAME_PROPERTY, dbSinkConfig.user);
    }
    if (dbSinkConfig.password != null) {
      configAccessor.getConfiguration().set(DBConfiguration.PASSWORD_PROPERTY, dbSinkConfig.password);
    }

    if (!Strings.isNullOrEmpty(dbSinkConfig.getTransactionIsolationLevel())) {
      configAccessor.setTransactionIsolationLevel(dbSinkConfig.getTransactionIsolationLevel());
    }

    context.addOutput(Output.of(dbSinkConfig.referenceName, new SinkOutputFormatProvider(ETLDBOutputFormat.class,
      configAccessor.getConfiguration())));
  }
	 */
	
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
		 * 
		 * The primary key of the table is important, as this [CrateSink] supports
		 * JDBC's DUPLICATE ON KEY feature to enable proper update requests in case
		 * of key conflicts.
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
	public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, CrateWritable>> emitter) throws Exception {
		emitter.emit(new KeyValue<NullWritable, CrateWritable>(null, new CrateWritable(connect, input)));
	}

	public static class CrateSinkConfig extends JdbcConfig {

		private static final long serialVersionUID = 5345965522745690011L;

		public static final String TABLE_NAME = "tableName";
		public static final String PRIMARY_KEY = "primaryKey";

		@Name(TABLE_NAME)
		@Description("Name of the Crate table to export data to.")
		@Macro
		public String tableName;

		@Name(PRIMARY_KEY)
		@Description("Name of the primary key of the Crate table to export data to.")
		@Macro
		public String primaryKey;

		public CrateSinkConfig() {
			super();
		}
		
		public String getEndpoint() {
			return String.format(Locale.ENGLISH, "jdbc:crate://%s:%s/", host, port);
		}

		public Properties getProps() {
			
			Properties properties = new Properties();
			
			if (user == null || password == null)
				return properties;
			
			properties.put("user", user);
			properties.put("password", password);
			
			return properties;
			
		}

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
