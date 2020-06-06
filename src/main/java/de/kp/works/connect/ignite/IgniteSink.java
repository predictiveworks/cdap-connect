package de.kp.works.connect.ignite;
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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("IgniteSink")
@Description("A batch sink to write structured records to an Apache Ignite cache.")
public class IgniteSink extends BatchSink<StructuredRecord, IgniteCacheWritable, NullWritable> {

	private IgniteSinkConfig config;
	
	public IgniteSink(IgniteSinkConfig config) {
		this.config = config;
	}
	
	@Override
	public void initialize(BatchRuntimeContext context) throws Exception {
		super.initialize(context);

		/* Initialize Apache Ignite context */
		IgniteContext.getInstance(config.getConfig());
		
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		super.configurePipeline(pipelineConfigurer);
	}
	
	@Override
	public void prepareRun(BatchSinkContext context) throws Exception {
		/*
		 * Check whether the provided cache already exists;
		 * if not, try to create the cache here
		 */
		IgniteContext instance = IgniteContext.getInstance();	
		Schema schema = context.getInputSchema();
		
		if (!instance.cacheExists(config.cacheName)) {
			/*
			 * The Ignite cache is created leveraging the 
			 */
			if (schema != null)
				instance.createCache(config.cacheName, config.cacheMode, schema);			
			
		}
		
		context.addOutput(Output.of(config.referenceName, new IgniteOutputFormatProvider(config)));
		
	}

	@Override
	public void transform(StructuredRecord input, Emitter<KeyValue<IgniteCacheWritable, NullWritable>> emitter) throws Exception {
		emitter.emit(new KeyValue<IgniteCacheWritable, NullWritable>(new IgniteCacheWritable(input), null));
	}

	private static class IgniteOutputFormatProvider implements OutputFormatProvider {

		private final Map<String, String> conf;

		IgniteOutputFormatProvider(IgniteSinkConfig config) {

			conf = new HashMap<>();
			
			conf.put(IgniteUtil.IGNITE_CACHE_NAME, config.cacheName);
			conf.put(IgniteUtil.IGNITE_CACHE_MODE, config.cacheMode);
			
		}

		/**
		 * The following method connect the output format provider with a specific
		 * output format and the dynamically delivered configuration
		 */
		@Override
		public String getOutputFormatClassName() {
			return IgniteOutputFormat.class.getName();
		}

		@Override
		public Map<String, String> getOutputFormatConfiguration() {
			return conf;
		}

	}

}
