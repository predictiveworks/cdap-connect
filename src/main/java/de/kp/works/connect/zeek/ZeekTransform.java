package de.kp.works.connect.zeek;
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;

import co.cask.cdap.api.data.format.StructuredRecord;
import de.kp.works.connect.EmptyFunction;
/*
 * [ZeekTransform] is the main class for transforming
 * Apache Kafka messages originating from osquery endpoint
 * monitor 
 */
public class ZeekTransform implements Function2<JavaRDD<ConsumerRecord<byte[], byte[]>>, Time, JavaRDD<StructuredRecord>> {

	private static final long serialVersionUID = -2256941762899970287L;

	private ZeekConfig config;
	
	public ZeekTransform(ZeekConfig config) {
		this.config = config;
	}
	
	@Override
	public JavaRDD<StructuredRecord> call(JavaRDD<ConsumerRecord<byte[], byte[]>> input, Time batchTime) throws Exception {
		
		if (input.isEmpty())
			return input.map(new EmptyFunction());
		
		Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> logTransform = new LogTransform(
				config, batchTime.milliseconds());

		return input.map(logTransform);
		
	}

}
