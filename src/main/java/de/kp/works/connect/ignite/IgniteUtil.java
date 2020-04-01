package de.kp.works.connect.ignite;
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

import org.apache.hadoop.conf.Configuration;

public class IgniteUtil {

	public static final String IGNITE_CACHE_NAME = "ignite.cache";
	public static final String IGNITE_FIELDS = "ignite.fields";
	public static final String IGNITE_PARTITIONS = "ignite.partitionS";
	
	public static String getCacheName(Configuration conf) {
		return conf.get(IGNITE_CACHE_NAME);
	}

	public static void setCacheName(Configuration conf, String cacheName) {
		conf.set(IGNITE_CACHE_NAME, cacheName);
	}
	
	public static String[] getFields(Configuration conf) {

		String fields = conf.get(IGNITE_FIELDS);
		if (fields == null || fields.equals(""))
			return null;

		else
			return fields.split(",");
	
	}

	public static void setFields(Configuration conf, String fields) {
		conf.set(IGNITE_FIELDS, fields);
	}
	
	public static int getPartitions(Configuration conf) {
		return Integer.valueOf(conf.get(IGNITE_PARTITIONS));
	}

	public static void setPartitions(Configuration conf, int partitions) {
		conf.set(IGNITE_PARTITIONS, String.valueOf(partitions));
	}
	
}
