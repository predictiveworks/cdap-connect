package de.kp.works.connect.mqtt;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.streaming.StreamingContext;

public class MQTTStreamUtil {
	
	private static final Logger LOG = LoggerFactory.getLogger(MQTTStreamUtil.class);

	static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context, MQTTConfig config) {
		return null;
	}

}
