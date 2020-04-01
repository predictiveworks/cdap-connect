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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;

public class IgniteRecordReader extends RecordReader<NullWritable, BinaryObject>
		implements org.apache.hadoop.mapred.RecordReader<NullWritable, BinaryObject> {

	private Iterator<List<?>> iterator;
	private int count = 0;

	private NullWritable key;
	private BinaryObject value;

	/* Default constructor used by the NEW API */
	public IgniteRecordReader() {
	}

	/* Constructor used by the OLD API */
	public IgniteRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
		reporter.setStatus(split.toString());
		init((IgniteSplit) split, job);
	}

	@Override
	public boolean next(NullWritable key, BinaryObject value) throws IOException {

		if (!iterator.hasNext()) {
			return false;
		}

		List<Object> values = new ArrayList<>(iterator.next());
		value = new BinaryObject(values);

		return true;

	}

	@Override
	public NullWritable createKey() {
		return null;
	}

	@Override
	public BinaryObject createValue() {
		return new BinaryObject();
	}

	@Override
	public long getPos() throws IOException {
		return count;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		init((IgniteSplit) split, conf);

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		/*
		 * Create dummy key and value and delegate to old API
		 */
		key = createKey();
		value = createValue();

		return next(key, value);

	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public BinaryObject getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() {
		/* This is not applicable */
		return 0;
	}

	@Override
	public void close() throws IOException {
		/* Do nothing */
		;
	}

	private void init(IgniteSplit split, Configuration conf) {

		/*
		 * STEP #1: Retrieve cache; note, the current implementation is restricted to
		 * IgniteCache<String, BinaryObject>
		 */
		String cacheName = IgniteUtil.getCacheName(conf);

		IgniteClient ignite = IgniteContext.getInstance().getClient();
		ClientCache<String, org.apache.ignite.binary.BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();
		/*
		 * STEP #2: Build SqlFieldsQuery
		 */
		String[] fieldNames = IgniteUtil.getFields(conf);

		StringBuilder sql = new StringBuilder();
		sql.append("SELECT _key, ");

		for (int i = 0; i < fieldNames.length; i++) {
			sql.append(fieldNames[i]);
			if (i != fieldNames.length - 1) {
				sql.append(", ");
			}
		}

		sql.append(" FROM ").append(cacheName);
		sql.append(" ORDER BY _key ASC ");

		sql.append(" LIMIT ").append(split.getLength());
		sql.append(" OFFSET ").append(split.getStart());

		SqlFieldsQuery query = new SqlFieldsQuery(sql.toString());

		iterator = cache.query(query).iterator();
	}

}
