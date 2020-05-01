package de.kp.works.connect.jdbc;
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
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public abstract class JdbcSource extends BatchSource<LongWritable, JdbcRecord, StructuredRecord> {

	protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);
	
	protected Class<? extends Driver> driverClass;

	protected abstract String getJdbcPluginID();

	protected abstract String getJdbcPluginName();

	protected abstract String getJdbcPluginType();

	protected abstract String getJdbcDriverName();
	
	protected abstract String getEndpoint();

	protected abstract String getUser();

	protected abstract String getPassword();

	protected abstract String getCountQuery();

	protected abstract String getInputQuery();

	protected abstract String getReferenceName();
	
	protected abstract void validate();
	
	protected Connection getConnection() throws SQLException {

		Connection connection;		
		
		String user = getUser();
		String password = getPassword();
		
		String endpoint = getEndpoint();
		
		if (user == null || password == null) {
			connection = DriverManager.getConnection(endpoint);

		} else {
			connection = DriverManager.getConnection(endpoint, user, password);

		}

		return connection;

	}

	/*
	 * The record schema this stage provides for subsequent
	 * analytics stages depends on the input query defined 
	 * by the user, e.g. because grouping and aggregation is 
	 * used.
	 * 
	 * Therefore the input query is used to retrieve a single 
	 * database row, as this result provides metadata for schema 
	 * inference. 
	 */	
	public Schema getSchema() throws Exception {

		Connection conn = null;
		Statement stmt = null;

		Schema schema = null;
		String inputQuery = getInputQuery();
		
		try {

			conn = getConnection();
			stmt = conn.createStatement();

			stmt.setMaxRows(1);

			ResultSet resultSet = stmt.executeQuery(inputQuery);
			schema = Schema.recordOf("jdbcSchema", JdbcUtils.getSchemaFields(resultSet));

		} catch (Exception e) {
			LOG.error("Schema retrieval with query = {} failed: {}", inputQuery, e.getMessage());

		} finally {

			if (stmt != null) stmt.close();
			if (conn != null) conn.close();

		}

		return schema;

	}
	
	@Override
	public void initialize(BatchRuntimeContext context) throws Exception {
		super.initialize(context);
		
		String jdbcPluginId = getJdbcPluginID();		
	    driverClass = context.loadPluginClass(jdbcPluginId);
		
	}
	
	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		super.configurePipeline(pipelineConfigurer);

		validate();

		try {

			String pluginId = getJdbcPluginID();
			
			String pluginName = getJdbcPluginName();
			String pluginType = getJdbcPluginType();
			
			driverClass = pipelineConfigurer.usePluginClass(pluginType, pluginName,
					pluginId, PluginProperties.builder().build());

			JdbcDriverShim driverShim = new JdbcDriverShim(driverClass.newInstance());
			DriverManager.registerDriver(driverShim);

			Schema schema = getSchema();
			pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);

		} catch (Exception e) {
			LOG.error(String.format("Configuring JdbcSource failed with: %s." , e.getLocalizedMessage()));

		}

	}

	@Override
	public void prepareRun(BatchSourceContext context) throws Exception {

		validate();
		
		String endpoint = getEndpoint();
		
		String user = getUser();
		String password = getPassword();

		String driverName = getJdbcDriverName();
		
		Configuration hadoopCfg = new Configuration();
		hadoopCfg.clear();

		
		if (user == null && password == null) {
			DBConfiguration.configureDB(hadoopCfg, driverName, endpoint);

		} else {
			DBConfiguration.configureDB(hadoopCfg, driverName, endpoint, user,
					password);

		}

		hadoopCfg.setInt(MRJobConfig.NUM_MAPS, 1);
		
		String countQuery = getCountQuery();
		String inputQuery = getInputQuery();
		
		String referenceName = getReferenceName();
		
		JdbcInputFormat.setInput(hadoopCfg, JdbcRecord.class, countQuery, inputQuery);
		context.setInput(Input.of(referenceName, new SourceInputFormatProvider(JdbcInputFormat.class, hadoopCfg)));

	}

	@Override
	public void transform(KeyValue<LongWritable, JdbcRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
		
		StructuredRecord record = input.getValue().getRecord();
		emitter.emit(record);
		
	}

}
