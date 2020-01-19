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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.SourceInputFormatProvider;


@Plugin(type = "batchsource")
@Name("CrateSource")
@Description("A batch source to read structured records from the Crate IoT scale database.")
public class CrateSource extends BatchSource<LongWritable, CrateRecord, StructuredRecord> {

	private static final Logger LOG = LoggerFactory.getLogger(CrateSource.class);
	
	private static final String JDBC_PLUGIN_ID = "source.jdbc.crate";
	/*
	 * 'type' and 'name' must match the provided JSON specification
	 */
	private static final String JDBC_PLUGIN_TYPE = "jdbc";
	private static final String JDBC_PLUGIN_NAME = "crate";

	private final CrateSourceConfig cfg;
	private final CrateConnect connect;
	
	private Class<? extends Driver> driverClass;

	public CrateSource(CrateSourceConfig crateConfig) {

		this.cfg = crateConfig;
		this.connect = new CrateConnect().setHost(cfg.host).setPort(cfg.port).setUser(cfg.user)
				.setPassword(cfg.password);

	}

	@Override
	public void initialize(BatchRuntimeContext context) throws Exception {
		super.initialize(context);
	    driverClass = context.loadPluginClass(JDBC_PLUGIN_ID);
		
	}
	
	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		super.configurePipeline(pipelineConfigurer);

		cfg.validate();

		try {

			driverClass = pipelineConfigurer.usePluginClass(JDBC_PLUGIN_TYPE, JDBC_PLUGIN_NAME,
					JDBC_PLUGIN_ID, PluginProperties.builder().build());

			JDBCDriverShim driverShim = new JDBCDriverShim(driverClass.newInstance());
			DriverManager.registerDriver(driverShim);

			Schema schema = getSchema();
			pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);

		} catch (Exception e) {
			LOG.error(String.format("Configuring CrateSource failed with: %s." , e.getLocalizedMessage()));

		}

	}

	@Override
	public void prepareRun(BatchSourceContext context) throws Exception {

		cfg.validate();
		connect.setInputQuery(cfg.getInputQuery());
		LOG.debug("inputQuery = {}; connectionString = {}", cfg.getInputQuery(), cfg.getConnectionString());

		Configuration hadoopCfg = new Configuration();
		hadoopCfg.clear();

		if (cfg.user == null && cfg.password == null) {
			DBConfiguration.configureDB(hadoopCfg, cfg.getDriverName(), cfg.getConnectionString());

		} else {
			DBConfiguration.configureDB(hadoopCfg, cfg.getDriverName(), cfg.getConnectionString(), cfg.user,
					cfg.password);

		}

		hadoopCfg.setInt(MRJobConfig.NUM_MAPS, 1);
		
		CrateInputFormat.setInput(hadoopCfg, CrateRecord.class, cfg.getInputCountQuery(), cfg.getInputQuery());
		context.setInput(Input.of(cfg.referenceName, new SourceInputFormatProvider(CrateInputFormat.class, hadoopCfg)));

	}

	@Override
	public void transform(KeyValue<LongWritable, CrateRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
		
		StructuredRecord record = input.getValue().getRecord();
		emitter.emit(record);
		
	}
	/**
	 * The record schema this stage provide for subsequent analytics
	 * stages depends on the SQL query defined by the user, e.g. because
	 * grouping and aggregation is used.
	 * 
	 * Therefore the query, stripped by potential CONDITION clauses, is
	 * used to retrieve a singel database row, as this result provides
	 * metadata for schema inference. 
	 */
	public Schema getSchema() throws Exception {

		Connection conn = null;
		Statement stmt = null;

		Schema schema = null;

		try {

			conn = connect.getConnection();
			stmt = conn.createStatement();

			stmt.setMaxRows(1);

			ResultSet resultSet = stmt.executeQuery(connect.getInputQuery());
			schema = Schema.recordOf("crateSchema", CrateUtils.getSchemaFields(resultSet));

		} catch (Exception e) {
			LOG.error("Schema retrieval with query = {} failed: {}", connect.getInputQuery(), e.getMessage());

		} finally {

			if (stmt != null) stmt.close();
			if (conn != null) conn.close();

		}

		return schema;

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
		@Name(TABLE_NAME)
		@Description("Name of the Crate table to import data from.")
		@Nullable
		@Macro
		public String tableName;

		@Name(INPUT_QUERY)
		@Description("The SQL select statement to import data from the Crate database. "
				+ "For example: select * from <your table name>'.")
		@Nullable
		@Macro
		String inputQuery;

		public CrateSourceConfig(String referenceName, String host, String port, @Nullable String user,
				@Nullable String password, @Nullable String tableName, @Nullable String inputQuery) {
			super(referenceName, host, port, user, password);

			this.tableName = tableName;
			this.inputQuery = inputQuery;

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
