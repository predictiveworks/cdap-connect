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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

import de.kp.works.connect.EmptyOutputCommiter;

public class AerospikeOutputFormat<K extends AerospikeWritable, V> extends OutputFormat<K, V> {

	private static final Logger LOG = LoggerFactory.getLogger(AerospikeOutputFormat.class);

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new EmptyOutputCommiter();
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		/*
		 * The configuration has been provided by the 
		 * Influx Output Format Provider
		 */
		Configuration conf = context.getConfiguration();
		
		Properties props = new Properties();
		props.setProperty(AerospikeConnect.AEROSPIKE_HOST, conf.get(AerospikeConnect.AEROSPIKE_HOST));
		
		props.setProperty(AerospikeConnect.AEROSPIKE_PORT, conf.get(AerospikeConnect.AEROSPIKE_PORT));
		props.setProperty(AerospikeConnect.AEROSPIKE_TIMEOUT, conf.get(AerospikeConnect.AEROSPIKE_TIMEOUT));
		
		props.setProperty(AerospikeConnect.AEROSPIKE_USER, conf.get(AerospikeConnect.AEROSPIKE_USER));
		props.setProperty(AerospikeConnect.AEROSPIKE_PASSWORD, conf.get(AerospikeConnect.AEROSPIKE_PASSWORD));
		
		AerospikeClient client = new AerospikeConnect().getClient(props);
		WritePolicy policy = new WritePolicy(client.writePolicyDefault);
		
		String writeMode =conf.get(AerospikeConnect.AEROSPIKE_WRITE);
		switch (writeMode) {
		case "ErrorIfExists": {
			policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
			break;
		}
		case "Ignore": {
			policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
			break;
		}
		case "Overwrite": {
			policy.recordExistsAction = RecordExistsAction.REPLACE;
			break;
		}
		case "Append": {
			policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
			break;
		}
		default: {
			/* Append */
			policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;			
		}
		}
		
		AerospikeRecordWriter writer = new AerospikeRecordWriter(client, policy);
		writer.setNamespace(conf.get(AerospikeConnect.AEROSPIKE_NAMESPACE));
		writer.setSetName(conf.get(AerospikeConnect.AEROSPIKE_SET));
		
		return writer;
	}

	public class AerospikeRecordWriter extends RecordWriter<K, V> {

		private AerospikeClient client;
		private WritePolicy policy;
		
		private String namespace;
		private String setName;
		
		public AerospikeRecordWriter(AerospikeClient client, WritePolicy policy) {
			this.client = client;
			this.policy = policy;
		}

		public void setNamespace(String namespace) {
			this.namespace = namespace;
		}

		public void setSetName(String setName) {
			this.setName = setName;
		}
		
		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			/*
			 * This method is used to close the connection
			 * to the Aerospike database
			 */
			try {
				client.close();
				
			} catch (Exception e) {

				String message = String.format("Closing connection to Aerospike failed: %s", e.getLocalizedMessage());
				LOG.error(message);
				
				throw new IOException(message);

			}
			
		}

		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			try {
				key.write(client, policy, namespace, setName);
				
			} catch (Exception e) {
				
				String message = String.format("Writing record to Aerospike failed: %s", e.getLocalizedMessage());
				LOG.error(message);
				
				throw new IOException(message);
			}
			
		}
		
	}

}
