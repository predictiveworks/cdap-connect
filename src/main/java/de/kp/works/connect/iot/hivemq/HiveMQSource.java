package de.kp.works.connect.iot.hivemq;
/*
 * Copyright (c) 2020 Dr. Krusche & Partner PartG. All rights reserved.
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

import java.util.Map;

import org.apache.spark.streaming.api.java.JavaDStream;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;

@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("HiveMQSource")
@Description("An MQTT streaming source that listens to a HiveMQ MQTT broker and subscribes to a given topic.")
public class HiveMQSource extends StreamingSource<StructuredRecord> {

	private static final long serialVersionUID = 1416177275229321806L;

	private HiveMQSourceConfig config;
	
	public HiveMQSource(HiveMQSourceConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		config.validate();

		/*
		 * __KUP__
		 * 
		 * We set the output schema explicitly to 'null' as the 
		 * schema is inferred dynamically from the provided events
		 */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		stageConfigurer.setOutputSchema(null);

	}
	
	@Override
	public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
		
		SecureStore secureStore = context.getSparkExecutionContext().getSecureStore();
		Map<String,String> secureData = secureStore.listSecureData(context.getNamespace());
		
		return HiveMQStreamUtil.getStructuredRecordJavaDStream(context, config, secureData);			}

}
