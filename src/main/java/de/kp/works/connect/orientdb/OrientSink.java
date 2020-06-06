package de.kp.works.connect.orientdb;
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
import java.util.Map;

import org.apache.hadoop.io.NullWritable;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
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
@Name("OrientSink")
@Description("A batch connector to write structured records to an OrientDB graph database. "
		+ "A structured record either describes a graph vertex or an edge. Use cases where "
		+ "records specify vertices and edges are supported by the GraphSink.")
public class OrientSink extends BatchSink<StructuredRecord, OrientGraphWritable, NullWritable> {

	private OrientSinkConfig config;
	
	public OrientSink(OrientSinkConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(BatchRuntimeContext context) throws Exception {
		super.initialize(context);
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		super.configurePipeline(pipelineConfigurer);
		
		config.validate();

	}
	
	@Override
	public void prepareRun(BatchSinkContext context) throws Exception {		
		context.addOutput(Output.of(config.referenceName, new OrientOutputFormatProvider(config)));
	}

	@Override
	public void transform(StructuredRecord input, Emitter<KeyValue<OrientGraphWritable, NullWritable>> emitter) throws Exception {
		emitter.emit(new KeyValue<OrientGraphWritable, NullWritable>(new OrientGraphWritable(input), null));
	}

	public static class OrientSinkConfig extends OrientConfig {

		private static final long serialVersionUID = 2327992793678581423L;

		@Description("The write options. Supported values are 'Append', 'Overwrite', 'Ignore' and 'ErrorIfExists'. Default is 'Append'.")
		@Macro
		public String writeOption;

		public OrientSinkConfig() {
			super();

			writeOption = "Append";

		}
		
		public void validate() {
			super.validate();
		}
	}

	private static class OrientOutputFormatProvider implements OutputFormatProvider {

		private final Map<String, String> conf;

		OrientOutputFormatProvider(OrientSinkConfig config) {

			this.conf = new HashMap<>();

			conf.put(OrientUtil.ORIENT_URL, config.getUrl());
			
			conf.put(OrientUtil.ORIENT_USER, config.user);
			conf.put(OrientUtil.ORIENT_PASSWORD, config.password);
			
			conf.put(OrientUtil.ORIENT_EDGE_TYPE, config.edgeType);
			conf.put(OrientUtil.ORIENT_VERTEX_TYPE, config.vertexType);
			
			conf.put(OrientUtil.ORIENT_WRITE, config.writeOption);

		}

		/**
		 * The following method connect the output format provider with a specific
		 * output format and the dynamically delivered configuration
		 */
		@Override
		public String getOutputFormatClassName() {
			return OrientOutputFormat.class.getName();
		}

		@Override
		public Map<String, String> getOutputFormatConfiguration() {
			return conf;
		}

	}
	
}
