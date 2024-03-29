package de.kp.works.connect.things.kafka;
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.util.HashMap;
import java.util.Map;

@Plugin(type = "batchsink")
@Name("ThingsHttpSink")
@Description("A Works Batch connector for writing structured (device) records to the ThingsBoard IoT platform via HTTP.")
public class ThingsHttpSink extends BatchSink<StructuredRecord, Void, Void> {

	private final ThingsSinkConfig config;
	private final ThingsClient client;

	public ThingsHttpSink(ThingsSinkConfig config) {

		this.config = config;
		this.client = new ThingsClient(config);

	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		super.configurePipeline(pipelineConfigurer);
		config.validate();
	}

	@Override
	public void prepareRun(BatchSinkContext context) {
		context.addOutput(Output.of(config.referenceName, new ThingsboardOutputFormatProvider()));
	}

	@Override
	public void transform(StructuredRecord input, Emitter<KeyValue<Void, Void>> emitter) throws Exception {
		config.validate();
		client.sendTelemetryToAsset(input);

	}

	/**
	 * Output format provider for Thingsboard Sink.
	 */
	private static class ThingsboardOutputFormatProvider implements OutputFormatProvider {
		private final Map<String, String> conf = new HashMap<>();

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
