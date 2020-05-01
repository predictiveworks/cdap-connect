package de.kp.works.connect.redshift;
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

import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import de.kp.works.connect.jdbc.JdbcDriverShim;
import de.kp.works.connect.jdbc.JdbcRecord;

@Plugin(type = "batchsource")
@Name("RedshiftSource")
@Description("A batch source to read structured records from an Amazon Redshift database.")
public class RedshiftSource extends BatchSource<NullWritable, JdbcRecord, StructuredRecord> {

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
	
	public RedshiftSource(RedshiftSourceConfig config) {
		this.config = config;
	}
	
	@Override
	public void initialize(BatchRuntimeContext context) throws Exception {
		super.initialize(context);
	    driverClass = context.loadPluginClass(JDBC_PLUGIN_ID);
		
	}
	
	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		super.configurePipeline(pipelineConfigurer);

		config.validate();

		try {

			driverClass = pipelineConfigurer.usePluginClass(JDBC_PLUGIN_TYPE, JDBC_PLUGIN_NAME,
					JDBC_PLUGIN_ID, PluginProperties.builder().build());

			JdbcDriverShim driverShim = new JdbcDriverShim(driverClass.newInstance());
			DriverManager.registerDriver(driverShim);

			Schema schema = getSchema();
			pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);

		} catch (Exception e) {
			LOG.error(String.format("Configuring RedshiftSource failed with: %s." , e.getLocalizedMessage()));

		}

	}

	@Override
	public void prepareRun(BatchSourceContext context) throws Exception {
		// TODO Auto-generated method stub
		
	}
	/**
	 * The record schema this stage provides for subsequent analytics
	 * stages depends on the SQL query defined by the user, e.g. because
	 * grouping and aggregation is used.
	 * 
	 * Therefore the query is used to retrieve a single database row, 
	 * as this result provides metadata for schema inference. 
	 */
	public Schema getSchema() throws Exception {
		return null;
	}

}
