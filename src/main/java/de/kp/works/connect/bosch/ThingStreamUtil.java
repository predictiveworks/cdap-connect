package de.kp.works.connect.bosch;
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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.streaming.StreamingContext;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.stream.ws.*;

public class ThingStreamUtil {

	static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context, ThingConfig config) {
		return WSStreamUtils.createDirectStream(context.getSparkStreamingContext()).transform(new RecordTransform());
		
	}

	public static class RecordTransform implements Function<JavaRDD<String>, JavaRDD<StructuredRecord>> {

		private static final long serialVersionUID = -4426828579132235453L;

		@Override
		public JavaRDD<StructuredRecord> call(JavaRDD<String> v1) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
		
	}

	private ThingStreamUtil() {
		// no-op
	}

}
