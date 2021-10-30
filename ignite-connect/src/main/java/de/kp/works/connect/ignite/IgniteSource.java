package de.kp.works.connect.ignite;
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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;

@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("IgniteSource")
@Description("A batch connector plugin to read structured data records from Apache Ignite caches "
		+ "and transform them into structured pipeline records.")
public class IgniteSource extends BatchSource<NullWritable, BinaryObject, StructuredRecord> {

	private static final Logger LOG = LoggerFactory.getLogger(IgniteSource.class);
	/*
	 * Reference to the inferred schema
	 */
	private Schema outputSchema;
	private final IgniteSourceConfig config;
	
	public IgniteSource(IgniteSourceConfig config) {
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

		config.validate();

		try {
			/*
			 * Infer schema from dummy query request; this mechanism
			 * also checks whether an Apache Ignite connection can be
			 * established or not. This is done prior any data processing
			 */
			outputSchema = config.getSchema(config.fieldNames);
			pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);

		} catch (Exception e) {
			LOG.error(String.format("Configuring IgniteSource failed with: %s." , e.getLocalizedMessage()));

		}

	}

	@Override
	public void prepareRun(BatchSourceContext context) throws Exception {

		Job job = JobUtils.createInstance();
		Configuration conf = job.getConfiguration();

		job.setSpeculativeExecution(false);
		/*
		 * Prepare Hadoop configuration
		 *
		 * - cacheName
		 * 
		 * The Ignite connection is implemented
		 * as a singleton and has been configured
		 * and initialized already (see above)
		 */		
		IgniteUtil.setCacheName(conf, config.cacheName);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(BinaryObject.class);
		
		context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(IgniteInputFormat.class, conf)));
		
	}

	@Override
	public void transform(KeyValue<NullWritable, BinaryObject> input, Emitter<StructuredRecord> emitter) {
		/*
		 * Access to Apache Ignite exposes data as [BinaryObject]
		 * and this method transforms each object into a CDAP record
		 */
		List<Object> values = input.getValue().getValues();
		
		StructuredRecord record = config.values2Record(values, outputSchema);
		emitter.emit(record);
		
	}

}
