package de.kp.works.connect.pubsub;
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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.ArrayList;
import java.util.List;

@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("PubSubSource")
@Description("A Works streaming source for reading real-time events from Google PubSub," +
			" and transforming them into structured data flow records.")
public class PubSubSource extends StreamingSource<StructuredRecord> {

	private static final long serialVersionUID = 5755422000062454135L;

	private final PubSubConfig config;

	public PubSubSource(PubSubConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		super.configurePipeline(pipelineConfigurer);
		
		config.validate();
		pipelineConfigurer.getStageConfigurer().setOutputSchema(getSchema());

	}

	@Override
	public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
		
		SecureStore secureStore = context.getSparkExecutionContext().getSecureStore();
		List<SecureStoreMetadata> secureData = secureStore.list(context.getNamespace());
		
		return PubSubStreamUtil.getStructuredRecordJavaDStream(context, config, secureData, getSchema());			

	}

	private Schema getSchema() {

		List<Schema.Field> fields = new ArrayList<>();

		fields.add(Schema.Field.of("id", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG)));

		fields.add(Schema.Field.of("attributes",
				Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));
		fields.add(Schema.Field.of("message", Schema.of(Schema.Type.BYTES)));

		return Schema.recordOf("pubSubSchema", fields);

	}
	
}
