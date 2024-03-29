package de.kp.works.connect.shopify;
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

import de.kp.works.connect.common.NoSplit;
import de.kp.works.connect.common.http.page.HttpPage;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;

import java.util.Collections;
import java.util.List;

public class ShopifyInputFormat extends InputFormat<NullWritable, HttpPage>
		implements org.apache.hadoop.mapred.InputFormat<NullWritable, HttpPage> {

	/***** NEW API *****/

	@Override
	public List<InputSplit> getSplits(JobContext context) {
		return Collections.singletonList(new NoSplit());
	}

	@Override
	public RecordReader<NullWritable, HttpPage> createRecordReader(InputSplit inputSplit,
			TaskAttemptContext taskAttemptContext) {
		return new ShopifyRecordReader();
	}

	@Override
	public org.apache.hadoop.mapred.RecordReader<NullWritable, HttpPage> getRecordReader(
			org.apache.hadoop.mapred.InputSplit split, JobConf conf, Reporter reporter) {
		return new ShopifyRecordReader();
	}

	@Override
	public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf conf, int numSplit) {
		return new NoSplit[]{ new NoSplit() };
	}
}
