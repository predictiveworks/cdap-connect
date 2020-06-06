package de.kp.works.connect.kafka.zeek;
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

import org.apache.spark.streaming.api.java.JavaDStream;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;

@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("ZeekSource")
@Description("An Apache Kafka streaming source that supports real-time events that originate from Zeek.")
public class ZeekSource extends StreamingSource<StructuredRecord> {

	private static final long serialVersionUID = -1768880702434233235L;
	private ZeekConfig config;
	
	public ZeekSource(ZeekConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		config.validate();
		/*
		 * We set the output schema explicitly to 'null' as the 
		 * schema is inferred dynamically from the provided format
		 */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		stageConfigurer.setOutputSchema(null);
		
		if (config.getMaxRatePerPartition() != null && config.getMaxRatePerPartition() > 0) {

			Map<String, String> pipelineProperties = new HashMap<>();

			pipelineProperties.put("spark.streaming.kafka.maxRatePerPartition",
					config.getMaxRatePerPartition().toString());
			pipelineConfigurer.setPipelineProperties(pipelineProperties);

		}
	}
	
	@Override
	public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
		return ZeekStreamUtil.getStructuredRecordJavaDStream(context, config);			}

}
