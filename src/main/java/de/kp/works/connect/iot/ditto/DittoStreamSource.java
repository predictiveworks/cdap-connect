package de.kp.works.connect.iot.ditto;
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
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;

@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("DittoSource")
@Description("An Eclipse Ditto IoT streaming source that supports real-time websocket event streams.")
public class DittoStreamSource extends StreamingSource<StructuredRecord>{

	private static final long serialVersionUID = -8154749515870756082L;
	
	private final DittoConfig config;
	
	public DittoStreamSource(DittoConfig config) {
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
		
		context.registerLineage(config.referenceName);
		return DittoStreamUtil.getStructuredRecordJavaDStream(context, config);
		
	}

}
