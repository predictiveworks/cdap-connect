package de.kp.works.connect.aerospike;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;

@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("AerospikeSource")
@Description("A batch connector plugin to read data records from Aerospike namespaces and "
	      + "sets and to transform into structured pipeline records.")
public class AerospikeSource extends BatchSource<AerospikeEntry.Key, AerospikeEntry.Record, StructuredRecord> {
	/*
	 * [AerospikeSource] supports PredictiveWorks. low latency strategy.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AerospikeSource.class);
	/*
	 * Reference to the inferred schema
	 */
	private Schema outputSchema;
	private AerospikeSourceConfig config;
	
	public AerospikeSource(AerospikeSourceConfig config) {
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

		try {
			/*
			 * Infer schema from dummy query request; this mechanism
			 * also checks whether an Aerospike connection can be
			 * established or not
			 */
			outputSchema = config.getSchema();
			pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);

		} catch (Exception e) {
			LOG.error(String.format("Configuring AerospikeSource failed with: %s." , e.getLocalizedMessage()));

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
		 * - operation (*)
		 * - nodeName (+)
		 * - host (*)
		 * - port (*)
		 * - namespace (*)
		 * - setName (*)
		 * - user (*)
		 * - password (*)
		 * - binLength (+)
		 * - bin ... (+)
		 * - numRangeBin (+)
		 * - numRangeBegin
		 * - numRangeEnd
		 */		
		AerospikeUtil.setHost(conf, config.host);
		AerospikeUtil.setPort(conf, config.port);

		AerospikeUtil.setNamespace(conf, config.namespace);
		AerospikeUtil.setSetName(conf, config.setname);
		
		AerospikeUtil.setUser(conf, config.user);
		AerospikeUtil.setPassword(conf, config.password);
		
		/* Extract bins */
		List<String> bins = new ArrayList<>();
		for (Schema.Field field: outputSchema.getFields()) {
			bins.add(field.getName());
		}
		
		AerospikeUtil.setBins(conf, String.join(",", bins));
		/*
		 * The current implementation restricts read
		 * operations to 'scans'; parameters that refer
		 * to 'numrange' (query) operations are set to
		 * initial values
		 */
		AerospikeUtil.setOperation(conf, "scan");
		AerospikeUtil.setNumRangeBin(conf, "");
		
		AerospikeUtil.setNumRangeBegin(conf, 0L);
		AerospikeUtil.setNumRangeEnd(conf, 0L);

		job.setMapOutputKeyClass(AerospikeEntry.Key.class);
		job.setMapOutputValueClass(AerospikeEntry.Record.class);
		
		context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(AerospikeInputFormat.class, conf)));
		
	}

	@Override
	public void transform(KeyValue<AerospikeEntry.Key, AerospikeEntry.Record> input, Emitter<StructuredRecord> emitter) throws Exception {

		Map<String, Object> bins = input.getValue().getRecord().bins;
		
		StructuredRecord record = config.bins2Record(bins, outputSchema);
		emitter.emit(record);
		
	}

	public static class AerospikeSourceConfig extends AerospikeConfig {

		private static final long serialVersionUID = -1829167746036753033L;
		
		public AerospikeSourceConfig() {
			super();
		}
		
		public void validate() {
			super.validate();
		}
		
	}
}
