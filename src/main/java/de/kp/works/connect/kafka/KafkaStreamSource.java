package de.kp.works.connect.kafka;

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

import org.apache.spark.streaming.api.java.JavaDStream;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.common.IdUtils;
import de.kp.works.connect.KafkaStreamConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * __KUP__
 * 
 * This is an adoption of CDAP's implementation of a Kafka plugin
 * that is capable to infer the data schema from the events provided
 * 
 * Kafka Streaming source.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("KafkaSTStream")
@Description("An Apache Kafka streaming source that supports real-time events that refer to a single topic.")
public class KafkaStreamSource extends StreamingSource<StructuredRecord> {

	private static final long serialVersionUID = -1344898376371260838L;
	private final KafkaStreamConfig conf;
	
	public KafkaStreamSource(KafkaStreamConfig conf) {
		this.conf = conf;
		
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		conf.validate();
		/*
		 * __KUP__
		 * 
		 * We set the output schema explicitly to 'null' as the 
		 * schema is inferred dynamically from the provided events
		 */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		stageConfigurer.setOutputSchema(null);
		
		if (conf.getMaxRatePerPartition() != null && conf.getMaxRatePerPartition() > 0) {

			Map<String, String> pipelineProperties = new HashMap<>();

			pipelineProperties.put("spark.streaming.kafka.maxRatePerPartition",
					conf.getMaxRatePerPartition().toString());
			pipelineConfigurer.setPipelineProperties(pipelineProperties);

		}
	}

	@Override
	public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
		
		context.registerLineage(conf.referenceName);
		return KafkaStreamUtil.getStructuredRecordJavaDStream(context, conf);
		
	}
	
}
