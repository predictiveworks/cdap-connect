package de.kp.works.connect.aerospike;
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
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.AerospikeException.ScanTerminated;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

public class AerospikeInputFormat extends InputFormat<AerospikeEntry.Key, AerospikeEntry.Record>
		implements org.apache.hadoop.mapred.InputFormat<AerospikeEntry.Key, AerospikeEntry.Record> {

	/***** NEW API *****/

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {

		Configuration cfg = context.getConfiguration();
		JobConf conf = cfg instanceof org.apache.hadoop.mapred.JobConf ? (org.apache.hadoop.mapred.JobConf) cfg
				: new org.apache.hadoop.mapred.JobConf(cfg);

		return Arrays.asList((InputSplit[]) getSplits(conf, conf.getNumMapTasks()));

	}

	@Override
	public RecordReader<AerospikeEntry.Key, AerospikeEntry.Record> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		/* NEW API */
		return new AerospikeRecordReader();
	}

	/***** OLD API *****/

	@Override
	public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {

		try {

			/* STEP #1: Build Aerospike Client 
			 * 
			 * - host
			 * - port
			 * - user
			 * - password 
			 */
			String host = AerospikeUtil.getHost(conf);
			int port = AerospikeUtil.getPort(conf);

			ClientPolicy policy = new ClientPolicy();
			policy.user = AerospikeUtil.getUser(conf);
			policy.password = AerospikeUtil.getPassword(conf);
			
			AerospikeClient client = AerospikeClientSingleton.getInstance(policy, host, port);

			/* STEP #2: Determine nodes */

			Node[] nodes = client.getNodes();
			int numNodes = nodes.length;
			if (numNodes == 0) {
				throw new IOException("No Aerospike nodes found.");
			}

			/* SETP #3: Generate splits */

			AerospikeSplit[] splits = new AerospikeSplit[numNodes];
			for (int i = 0; i < numNodes; i++) {

				Node node = nodes[i];
				splits[i] = new AerospikeSplit(node.getName(), conf);

			}

			return splits;

		} catch (Exception e) {
			throw new IOException(String.format("Method 'getSplits' failed: %s", e.getLocalizedMessage()));
		}

	}

	@Override
	public org.apache.hadoop.mapred.RecordReader<AerospikeEntry.Key, AerospikeEntry.Record> getRecordReader(org.apache.hadoop.mapred.InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		/* OLD API */
		return new AerospikeRecordReader(split, job, reporter);
	}

	/** RECORD READER **/

	public class AerospikeRecordReader extends RecordReader<AerospikeEntry.Key, AerospikeEntry.Record>
			implements org.apache.hadoop.mapred.RecordReader<AerospikeEntry.Key, AerospikeEntry.Record> {

		private AerospikeEntry.Key currentKey;
		private AerospikeEntry.Record currentValue;
		/*
		 * A helper class to fuse key & record 
		 * information retrieved from Aerospike
		 */
		public class KeyRecord {
			
			public Key key;
			public Record record;

			public KeyRecord(Key key, Record record) {
				this.key = key;
				this.record = record;
			}
			
			public Key getKey() {
				return key;
			}
			
			public Record getRecord() {
				return record;
			}
		}

		/*
		 * Thie record reader supports two different read 
		 * operations to retrieve data from an Aerospike 
		 * database
		 */
		private ScanReader scanReader = null;
		private QueryReader queryReader = null;
		/*
		 * Flags to control the scan or query reader
		 */
		private boolean isFinished = false;
		private boolean isError = false;
		private boolean isRunning = false;

		private ArrayBlockingQueue<KeyRecord> queue = new ArrayBlockingQueue<>(16 * 1024);

		/* Default constructor used by the NEW API */
		public AerospikeRecordReader() {
		}

		/* Constructor used by the OLD API */
		public AerospikeRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
			reporter.setStatus(split.toString());
			init((AerospikeSplit) split);
		}
		
		/** QUERY & SCANNER **/
		
		public class CallBack implements ScanCallback {
			@Override
			public void scanCallback(Key key, Record record) throws AerospikeException {

				try {
					queue.put(new KeyRecord(key, record));

				} catch (Exception e) {
					throw new ScanTerminated(e);
				}
			}
		}

		public class QueryReader extends java.lang.Thread {

			private Configuration conf;
			private String node;

			public QueryReader(String node, Configuration conf) {
				this.conf = conf;
				this.node = node;
			}

			@Override
			public void run() {
				try {

					/* STEP #1: Retrieve Aerospike Client */
					
					String host = AerospikeUtil.getHost(conf);
					int port = AerospikeUtil.getPort(conf);

					AerospikeClient client = AerospikeClientSingleton.getInstance(new ClientPolicy(), host, port);
					
					/* STEP #2: Build query statement */
					
					Statement stmt = new Statement();
					String namespace = AerospikeUtil.getNamespace(conf);
					stmt.setNamespace(namespace);
					
					String setName = AerospikeUtil.getSetName(conf);
					stmt.setSetName(setName);
					
					String numRangeBin = AerospikeUtil.getNumRangeBin(conf);
					Long numRangeBegin = AerospikeUtil.getNumRangeBegin(conf);
					Long numRangeEnd = AerospikeUtil.getNumRangeEnd(conf);
					
					stmt.setFilter(Filter.range(numRangeBin, numRangeBegin, numRangeEnd));
					
					String[] bins = AerospikeUtil.getBins(conf);
					if (bins != null) stmt.setBinNames(bins);
					
					/* STEP #3: Evaluate query result and send to queue */
					
					QueryPolicy queryPolicy = new QueryPolicy();
					RecordSet rs = client.queryNode(queryPolicy, stmt, client.getNode(node));
					
					isRunning = true;
					
					try {

						while (rs.next()) {
							
							Key key = rs.getKey();
							Record record = rs.getRecord();
							queue.put(new KeyRecord(key, record));
						
						}
						
					} finally {
						
						rs.close();
						isFinished = true;
					}
					
				} catch (Exception e) {

					isError = true;
					return;
				
				}
			}
		}
		
		public class ScanReader extends java.lang.Thread {

			private Configuration conf;
			private String node;
			
			public ScanReader(String node, Configuration conf) {
				this.conf = conf;
				this.node = node;
			}
			
			@Override
			public void run() {
				
				try {
					
					/* STEP #1: Retrieve Aerospike Client */
					
					String host = AerospikeUtil.getHost(conf);
					int port = AerospikeUtil.getPort(conf);

					AerospikeClient client = AerospikeClientSingleton.getInstance(new ClientPolicy(), host, port);
					
					/* STEP #2: Prepare Scan */
					
					ScanPolicy scanPolicy = new ScanPolicy();
					CallBack cb = new CallBack();

					String namespace = AerospikeUtil.getNamespace(conf);
					String setName = AerospikeUtil.getSetName(conf);
					
					isRunning = true;
					
					String[] bins = AerospikeUtil.getBins(conf);
					if (bins != null) {
						client.scanNode(scanPolicy, client.getNode(node), namespace, setName, cb, bins);

					} else
						client.scanNode(scanPolicy, client.getNode(node), namespace, setName, cb);

					isFinished = true;

				} catch (Exception e) {
					isError = true;
					return;
				}
			}
			
		}
		
		/** RECORD READER **/
		
		@Override
		public AerospikeEntry.Key createKey() {
			AerospikeEntry entry = new AerospikeEntry();
			return entry.newKey();
		}

		@Override
		public AerospikeEntry.Record createValue() {
			AerospikeEntry entry = new AerospikeEntry();
			return entry.newRecord();
		}

		@Override
		public long getPos() throws IOException {
			return 0;
		}

		@Override
		public boolean next(AerospikeEntry.Key key, AerospikeEntry.Record value) throws IOException {

			final int waitMS = 1000;
			int trials = 5;

			try {
				KeyRecord entry;
				while (true) {
					
					if (isError) return false;

					if (!isRunning) {
						Thread.sleep(100);
						continue;
					}

					if (!isFinished && queue.size() == 0) {
						if (trials == 0) {
							return false;
						}
						Thread.sleep(waitMS);
						trials--;

					} else if (isFinished && queue.size() == 0) {
						return false;
					
					} else if (queue.size() != 0) {
						entry = queue.take();
						break;
					}
				}

				currentKey = setCurrentKey(currentKey, entry.key);
				currentValue = setCurrentValue(currentValue, entry.record);

			} catch (Exception e) {
				throw new IOException(String.format("Reading next key value pair from queue failed: %s", e.getLocalizedMessage()));
			
			}

			return true;
		
		}

		private AerospikeEntry.Key setCurrentKey(AerospikeEntry.Key currentKey, Key key) {

			if (currentKey == null) {
				currentKey = new AerospikeEntry().newKey(key);
			}

			return currentKey;

		}

		private AerospikeEntry.Record setCurrentValue(AerospikeEntry.Record currentValue, Record record) {

			if (currentValue == null) {
				currentValue = new AerospikeEntry().newRecord(record);
			}

			return currentValue;
		}

		@Override
		public void close() throws IOException {

			if (scanReader != null) {

				try {
					scanReader.join();

				} catch (Exception e) {
					throw new IOException(String.format("Closing Scan Reader failed: %s", e.getLocalizedMessage()));
				}
				
				scanReader = null;
			
			}
			
			if (queryReader != null) {
				
				try {
					queryReader.join();
				
				} catch (Exception e) {
					throw new IOException(String.format("Closing Query Reader failed: %s", e.getLocalizedMessage()));
				
				}
				
				queryReader = null;
			
			}
		}

		@Override
		public AerospikeEntry.Key getCurrentKey() throws IOException, InterruptedException {
			return currentKey;
		}

		@Override
		public AerospikeEntry.Record getCurrentValue() throws IOException, InterruptedException {
			return currentValue;
		}

		@Override
		public float getProgress() {
			if (isFinished)
				return 1.0f;
			else
				return 0.0f;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			init((AerospikeSplit) split);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			/*
			 * NEW API call is routed to the OLD API; under the new one always create new
			 * objects since consumers can (and sometimes will) modify them
			 */
			if (currentKey == null) {
				currentKey = createKey();
			}
			
			if (currentValue == null) {
				currentValue = createValue();
			}

			return next(currentKey, currentValue);
		}

		private void init(AerospikeSplit split) {

			Configuration conf = split.getConf();
			
			String operation = AerospikeUtil.getOperation(conf);
			if (operation.equals("scan")) {
				
				scanReader = new ScanReader(split.getNode(), conf);
				scanReader.start();

			} else if (operation.equals("numrange")) {
				
				queryReader = new QueryReader(split.getNode(), conf);
				queryReader.start();
			
			}

		}

	}

}
