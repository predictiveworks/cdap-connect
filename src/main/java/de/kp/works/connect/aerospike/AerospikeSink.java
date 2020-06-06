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
@Name("AerospikeSink")
@Description("A batch sink to write structured records to an Aerospike database. The Aerospike key "
		+ "is derived from the record field values, and each bin of the Aerospike record represents a field.")
public class AerospikeSink extends BatchSink<StructuredRecord, AerospikeRecordWritable, NullWritable> {

	private AerospikeSinkConfig config;
	
	public AerospikeSink(AerospikeSinkConfig config) {
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
		context.addOutput(Output.of(config.referenceName, new AerospikeOutputFormatProvider(config)));
	}

	@Override
	public void transform(StructuredRecord input, Emitter<KeyValue<AerospikeRecordWritable, NullWritable>> emitter) throws Exception {
		emitter.emit(new KeyValue<AerospikeRecordWritable, NullWritable>(new AerospikeRecordWritable(input), null));
	}

	public static class AerospikeSinkConfig extends AerospikeConfig {

		private static final long serialVersionUID = -2710562181696331578L;

		@Description("The expiration time of database records in milliseconds. Default is 0 (no expiration).")
		@Macro
		public Integer expiration;

		@Description("The write options. Supported values are 'Append', 'Overwrite', 'Ignore' and 'ErrorIfExists'. Default is 'Append'.")
		@Macro
		public String writeOption;
		
		public AerospikeSinkConfig() {	
			super();
			
			expiration = 0;
			writeOption = "Append";
			
		}
		
		public void validate() {
			super.validate();
		}
		
	}

	private static class AerospikeOutputFormatProvider implements OutputFormatProvider {

		private final Map<String, String> conf;

		AerospikeOutputFormatProvider(AerospikeSinkConfig config) {

			this.conf = new HashMap<>();

			conf.put(AerospikeUtil.AEROSPIKE_HOST, config.host);
			conf.put(AerospikeUtil.AEROSPIKE_PORT, String.valueOf(config.port));
			
			conf.put(AerospikeUtil.AEROSPIKE_TIMEOUT, String.valueOf(config.timeout));
			conf.put(AerospikeUtil.AEROSPIKE_EXPIRATION, String.valueOf(config.expiration));
			
			conf.put(AerospikeUtil.AEROSPIKE_NAMESPACE, config.namespace);
			conf.put(AerospikeUtil.AEROSPIKE_SET, config.setname);
			
			conf.put(AerospikeUtil.AEROSPIKE_USER, config.user);
			conf.put(AerospikeUtil.AEROSPIKE_PASSWORD, config.password);
			
			conf.put(AerospikeUtil.AEROSPIKE_WRITE, config.writeOption);

		}

		/**
		 * The following method connect the output format provider with a specific
		 * output format and the dynamically delivered configuration
		 */
		@Override
		public String getOutputFormatClassName() {
			return AerospikeOutputFormat.class.getName();
		}

		@Override
		public Map<String, String> getOutputFormatConfiguration() {
			return conf;
		}

	}
	
}
