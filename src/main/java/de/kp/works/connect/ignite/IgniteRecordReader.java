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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class IgniteRecordReader extends RecordReader<NullWritable, BinaryObject>
		implements org.apache.hadoop.mapred.RecordReader<NullWritable, BinaryObject> {


	/* Default constructor used by the NEW API */
	public IgniteRecordReader() {
	}

	/* Constructor used by the OLD API */
	public IgniteRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
		reporter.setStatus(split.toString());
		init((IgniteSplit) split);
	}
	
	@Override
	public boolean next(NullWritable key, BinaryObject value) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public NullWritable createKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BinaryObject createValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		init((IgniteSplit) split);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BinaryObject getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getProgress() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	private void init(IgniteSplit split) {
		// TODO
	}

}
