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

import java.util.Properties;
import java.util.Set;

import co.cask.cdap.etl.api.streaming.StreamingContext;

public class BaseMqttUtil {
	/*
	 * A helper method to set streaming properties danymically
	 */
	public static void setSparkStreamingConf(StreamingContext context, Properties properties) {
		
		org.apache.spark.streaming.StreamingContext ssc = context.getSparkStreamingContext().ssc();
		Set<Object> keys = properties.keySet();

		for (Object key: keys) {
			
			String k = (String)key;
			String v = properties.getProperty(k);
			ssc.conf().set(k, v);
			
		}
		
	}
}
