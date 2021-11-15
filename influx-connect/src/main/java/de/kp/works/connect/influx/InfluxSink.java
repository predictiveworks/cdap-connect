package de.kp.works.connect.influx;
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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("InfluxSink")
@Description("A batch connector form writing data records to an InfluxDB time series database. "
		+ "A structured data record is divided into numeric and other fields. Numeric field "
		+ "values are transformed into Doubles and persisted as a measurement field. String "
		+ "field values are persisted as tags, while all other fields are ignored.")
public class InfluxSink extends BatchSink<StructuredRecord, InfluxPointWritable, NullWritable> {

	private final InfluxConfig config;
	private final InfluxConnect connect;

	public InfluxSink(InfluxConfig config) {

		this.config = config;
		this.connect = new InfluxConnect().setConnection(config.getConnectionString()).setDatabase(config.database)
				.setMeasurement(config.measurement).setUser(config.user).setPassword(config.password);

	}

	@Override
	public void initialize(BatchRuntimeContext context) throws Exception {
		super.initialize(context);
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		super.configurePipeline(pipelineConfigurer);
	}

	@Override
	public void prepareRun(BatchSinkContext context) throws Exception {
		/*
		 * STEP #1: Check whether the InfluxDB connection is valid
		 */
		if (!connect.isValidConnection())
			throw new IllegalArgumentException("Cannot establish connection with provided connection parameters.");

		/*
		 * STEP #2: Check whether provided database exists
		 */
		if (!connect.databaseExists()) {
			/*
			 * The database provided by the configuration does exist in the InfluxDB yet.
			 * The current implementation creates databases automatically.
			 */
			connect.createDatabase(config.duration, config.replication);
		}

		context.addOutput(Output.of(config.referenceName, new InfluxOutputFormatProvider(config)));

	}

	@Override
	public void transform(StructuredRecord input, Emitter<KeyValue<InfluxPointWritable, NullWritable>> emitter) {

		emitter.emit(new KeyValue<>(
				new InfluxPointWritable(input, config.measurement, config.timefield), null));

	}

	private static class InfluxOutputFormatProvider implements OutputFormatProvider {

		private final Map<String, String> conf;

		InfluxOutputFormatProvider(InfluxConfig config) {

			this.conf = new HashMap<>();
			/*
			 * Provide connection parameters & user credentials
			 */
			conf.put("influx.conn", config.getConnectionString());
			conf.put("influx.database", config.database);

			conf.put("influx.user", config.user);
			conf.put("influx.password", config.password);

		}

		/**
		 * The following method connect the output format provider with a specific
		 * output format and the dynamically delivered configuration
		 */
		@Override
		public String getOutputFormatClassName() {
			return InfluxOutputFormat.class.getName();
		}

		@Override
		public Map<String, String> getOutputFormatConfiguration() {
			return conf;
		}

	}

}
