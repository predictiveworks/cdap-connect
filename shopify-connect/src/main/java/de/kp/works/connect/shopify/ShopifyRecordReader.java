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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import de.kp.works.connect.common.http.page.HttpPage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class ShopifyRecordReader extends RecordReader<NullWritable, HttpPage> implements org.apache.hadoop.mapred.RecordReader<NullWritable, HttpPage> {

	private static final Gson gson = new GsonBuilder().create();

	private ShopifyPageIterator iterator;
	private HttpPage value;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
		
		Configuration conf = taskAttemptContext.getConfiguration();
		String json = conf.get(ShopifyInputFormatProvider.PROPERTY_CONFIG_JSON);
		
		ShopifyConfig config = gson.fromJson(json, ShopifyConfig.class);
		iterator = new ShopifyPageIterator(config);
		
	}

	@Override
	public boolean nextKeyValue() {
		
		if (!iterator.hasNext()) {
			return false;
		}
		
		value = iterator.next();
		return true;
	
	}

	@Override
	public NullWritable getCurrentKey() {
		return null;
	}

	@Override
	public HttpPage getCurrentValue() {
		return value;
	}

	@Override
	public float getProgress() {
		return 0.0f;
	}

	@Override
	public void close() throws IOException {
		if (iterator != null) {
			iterator.close();
		}
	}

	@Override
	public NullWritable createKey() {
		return null;
	}

	@Override
	public HttpPage createValue() {
		return null;
	}

	@Override
	public long getPos() {
		return 0;
	}

	@Override
	public boolean next(NullWritable key, HttpPage value) {
		return false;
	}
}
