package de.kp.works.connect.snowflake;
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

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.kp.works.connect.jdbc.JdbcOutputFormat;

public class SnowflakeOutputFormat<K, V extends SnowflakeWritable> extends JdbcOutputFormat<K, V> {

	private static final Logger LOG = LoggerFactory.getLogger(SnowflakeOutputFormat.class);

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

}
