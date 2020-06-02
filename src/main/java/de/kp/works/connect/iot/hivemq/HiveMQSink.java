package de.kp.works.connect.iot.hivemq;
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

import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("HiveMQSink")
@Description("Batch sink plugin to send messages to a HiveMQ MQTT server.")
public class HiveMQSink extends BatchSink<StructuredRecord, Void, Void> {

	/*
	 * CDAP (real-time) pipelines 
	 */
	
	private HiveMQSinkConfig config;
	private HiveMQUtil hiveMQUtil;
	
	public HiveMQSink(HiveMQSinkConfig config) {
		this.config = config;
		
		Map<String, String> secureData = new HashMap<>();
		this.hiveMQUtil = new HiveMQUtil(config, secureData);
		
	}
	
	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		super.configurePipeline(pipelineConfigurer);
		
		config.validate();
	}

	@Override
	public void prepareRun(BatchSinkContext context) throws Exception {		
		context.addOutput(Output.of(config.referenceName, new HiveMQOutputFormatProvider()));
	}

	@Override
	public void transform(StructuredRecord input, Emitter<KeyValue<Void, Void>> emitter) throws Exception {
		
		config.validate();
		hiveMQUtil.publish(input);

	}

	/**
	 * Output format provider for HiveMQ Sink.
	 */
	private static class HiveMQOutputFormatProvider implements OutputFormatProvider {
		private Map<String, String> conf = new HashMap<>();

		@Override
		public String getOutputFormatClassName() {
			return NullOutputFormat.class.getName();
		}

		@Override
		public Map<String, String> getOutputFormatConfiguration() {
			return conf;
		}
	}

}
