package de.kp.works.connect.orientdb;
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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

import de.kp.works.connect.EmptyOutputCommiter;

public class OrientOutputFormat<K extends OrientWritable, V> extends OutputFormat<K, V> {

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new EmptyOutputCommiter();
	}

	public class OrientRecordWriter extends RecordWriter<K,V> {

		private OrientFactory factory;
		private OrientGraphNoTx connection;
		
		public OrientRecordWriter(OrientFactory factory) {
			this.factory = factory;
			this.connection = factory.getConn();
		}
		
		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			/*
			 * This method is used to close the connection
			 * to the OrientDB database
			 */
			try {
				factory.closeConn();
				
			} catch (Exception e) {
				throw new IOException(String.format("Closing connection toOrientDB failed: %s", e.getLocalizedMessage()));
			}
			
		}

		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			try {

				key.write(connection);
				
			} catch (Exception e) {
				throw new IOException(String.format("Writing record to InfluxDB failed: %s", e.getLocalizedMessage()));
			}
			
		}
		
	}
	
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		/*
		 * The configuration has been provided by the 
		 * Orient Output Format Provider
		 */
		Configuration conf = context.getConfiguration();

		String conn = conf.get("orient.conn");
		
		String user = conf.get("orient.user");
		String password = conf.get("orient.password");

		try {
			
			OrientFactory factory = new OrientFactory(conn, user, password);
			return new OrientRecordWriter(factory);
			
		} catch (Exception e) {
			throw new IOException(String.format("Instantiating RecordWriter for OrientDB failed: %s", e.getLocalizedMessage()));
		}
		
	}

}
