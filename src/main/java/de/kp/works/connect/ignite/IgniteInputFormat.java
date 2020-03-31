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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class IgniteInputFormat extends InputFormat<NullWritable, BinaryObject>
		implements org.apache.hadoop.mapred.InputFormat<NullWritable, BinaryObject> {

	/***** NEW API *****/

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {

		Configuration cfg = context.getConfiguration();
		JobConf conf = cfg instanceof org.apache.hadoop.mapred.JobConf ? (org.apache.hadoop.mapred.JobConf) cfg
				: new org.apache.hadoop.mapred.JobConf(cfg);

		return Arrays.asList((InputSplit[]) getSplits(conf, conf.getNumMapTasks()));

	}

	@Override
	public RecordReader<NullWritable, BinaryObject> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new IgniteRecordReader();
	}

	/***** OLD API *****/

	@Override
	public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {

		IgniteSplit[] splits = null;
		try {
			/*
			 * This approach expects that the Ignite client
			 * is configured properly already elsewhere
			 */
			String cacheName = IgniteUtil.getCacheName(conf);
			IgniteContext context = IgniteContext.getInstance();
			/*
			 * Partitioned Mode
			 * 
			 * PARTITIONED mode is the most scalable distributed cache mode. 
			 * This is also the default cache mode. In this mode, the overall 
			 * data set is divided equally into partitions and all partitions 
			 * are split equally between participating nodes, essentially creating 
			 * one huge distributed store for data. 
			 * 
			 * This approach allows you to store as much data as can be fit in the 
			 * total memory (RAM and disk) available across all nodes.
			 */
			int partitions = context.getIgnite().affinity(cacheName).partitions();

			splits = new IgniteSplit[partitions];
			for (int i = 0; i < partitions; i++) {
				splits[i] = new IgniteSplit(cacheName, i);
			}
			
		} catch (Exception e) {
			throw new IOException(e.getLocalizedMessage());
		}
		
		return splits;
	}

	@Override
	public org.apache.hadoop.mapred.RecordReader<NullWritable, BinaryObject> getRecordReader(
			org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter) throws IOException {
		/* OLD API */
		return new IgniteRecordReader(split, job, reporter);
	}

}
