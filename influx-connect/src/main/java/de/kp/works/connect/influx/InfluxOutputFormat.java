package de.kp.works.connect.influx;
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
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

public class InfluxOutputFormat<K extends InfluxWritable, V> extends OutputFormat<K, V> {

	@Override
	public void checkOutputSpecs(JobContext context) {
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
		return new EmptyOutputCommiter();
	}

	public class InfluxRecordWriter extends RecordWriter<K,V> {

		private final InfluxDB influxDB;
		
		public InfluxRecordWriter(InfluxDB influxDB) {
			this.influxDB = influxDB;
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException {
			/*
			 * This method is used to close the connection
			 * to the InfluxDB database
			 */
			try {
				influxDB.close();
				
			} catch (Exception e) {
				throw new IOException(String.format("Closing connection to InfluxDB failed: %s", e.getLocalizedMessage()));
			}
			
		}

		@Override
		public void write(K key, V value) throws IOException {
			try {
				key.write(influxDB);
				
			} catch (Exception e) {
				throw new IOException(String.format("Writing record to InfluxDB failed: %s", e.getLocalizedMessage()));
			}
			
		}
		
	}
	
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException {
		/*
		 * The configuration has been provided by the 
		 * Influx Output Format Provider
		 */
		Configuration conf = context.getConfiguration();
		/*
		 * At this stage, there is no check whether the
		 * InfluxDB connection is valid or not; also no
		 * checks are performed whether database and
		 * measurement exist.
		 * 
		 * It is expected that this has been checked in a
		 * preparation phase already (Sink implementation).
		 * 
		 * Note, the InfluxDB measurement is sent to the
		 * InfluxWritable already and is irrelevant here
		 */
		String conn = conf.get("influx.conn");
		String database = conf.get("influx.database");
		
		String user = conf.get("influx.user");
		String password = conf.get("influx.password");

		try {
			/*
			 * The current implementation connects twice to the
			 * specified InfluxDB: first, when the pipeline stage
			 * is prepared, and second when the RecordWriter is
			 * initiated
			 */
			InfluxDB influxDB = InfluxDBFactory.connect(conn, user, password);
			influxDB.setDatabase(database);

			return new InfluxRecordWriter(influxDB);
			
		} catch (Exception e) {
			throw new IOException(String.format("Instantiating RecordWriter for InfluxDB failed: %s", e.getLocalizedMessage()));
		}
		
	}

}
