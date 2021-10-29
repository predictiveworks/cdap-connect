package de.kp.works.connect.opencti;
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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.List;

@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("CTISource")
@Description("A Threat Intelligence streaming source that listens to OpenCTI threat events.")
public class CTISource extends StreamingSource<StructuredRecord> {

	private static final long serialVersionUID = -8515614253262827164L;

	private final CTIConfig config;
	
	public CTISource(CTIConfig config) {
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
		List<SecureStoreMetadata> secureData = secureStore.list(context.getNamespace());
		
		return CTIStreamUtil.getStructuredRecordJavaDStream(context, config, secureData);

	}

}
