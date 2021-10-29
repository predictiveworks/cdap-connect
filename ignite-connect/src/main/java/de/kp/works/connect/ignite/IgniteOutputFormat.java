package de.kp.works.connect.ignite;
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

import java.io.IOException;

import de.kp.works.connect.common.EmptyOutputCommiter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgniteOutputFormat<K extends IgniteWritable, V> extends OutputFormat<K, V> {

	private static final Logger LOG = LoggerFactory.getLogger(IgniteOutputFormat.class);

	@Override
	public void checkOutputSpecs(JobContext context) {
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
		return new EmptyOutputCommiter();
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) {
		/*
		 * The configuration has been provided by the 
		 * Ignite Output Format Provider
		 */
		Configuration conf = context.getConfiguration();
		
		String cacheName = conf.get(IgniteUtil.IGNITE_CACHE_NAME);
		String cacheMode = conf.get(IgniteUtil.IGNITE_CACHE_MODE);
		
		return new IgniteRecordWriter(cacheName, cacheMode);
		
	}

	public class IgniteRecordWriter extends RecordWriter<K, V> {
		
		private final IgniteContext instance;

		private final String cacheName;
		private final String cacheMode;

		public IgniteRecordWriter(String cacheName, String cacheMode) {

			this.cacheName = cacheName;
			this.cacheMode = cacheMode;
			
			this.instance = IgniteContext.getInstance();

		}
		
		@Override
		public void write(K key, V value) throws IOException {
			
			try {
				key.write(instance, cacheName, cacheMode);
				
			} catch (Exception e) {
				
				String message = String.format("Writing record to Ignite cache failed: %s", e.getLocalizedMessage());
				LOG.error(message);
				
				throw new IOException(message);
			}
			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException {
			/*
			 * This method is used to close the connection
			 * to the Ignite cluster
			 */
			try {
				instance.close();
				
			} catch (Exception e) {

				String message = String.format("Closing connection to Ignite cluster failed: %s", e.getLocalizedMessage());
				LOG.error(message);
				
				throw new IOException(message);

			}
			
		}
		
	}

}
