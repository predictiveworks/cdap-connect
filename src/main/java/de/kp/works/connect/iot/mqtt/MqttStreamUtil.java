package de.kp.works.connect.iot.mqtt;
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

import java.util.List;

import org.apache.spark.streaming.api.java.JavaDStream;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.etl.api.streaming.StreamingContext;

import de.kp.works.stream.mqtt.*;

public class MqttStreamUtil extends BaseMqttStreamUtil {

	public static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context, MqttConfig mqttConfig, List<SecureStoreMetadata> mqttSecure) {

		JavaDStream<MqttEvent> stream = createStream(context, mqttConfig, mqttSecure);
		
		String format = mqttConfig.getFormat().name().toLowerCase();
		String[] topics = mqttConfig.getTopics();
		
		return stream.transform(new DefaultTransform(format, topics));
		
	}

}
