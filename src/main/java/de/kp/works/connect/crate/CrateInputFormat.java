package de.kp.works.connect.crate;
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
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import de.kp.works.connect.jdbc.JdbcRecord;
import de.kp.works.connect.jdbc.JdbcDriverShim;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public class CrateInputFormat<T extends JdbcRecord> extends DBInputFormat<T> implements Configurable {

	private static final Logger LOG = LoggerFactory.getLogger(CrateInputFormat.class);

	private Driver driver;
	private JdbcDriverShim driverShim;

	@Override
	/*
	 * This is an internal method to retrieve the Crate database connection from the
	 * provided configuration
	 */
	public Connection getConnection() {

		Configuration conf = getConf();

		Connection connection;
		try {

			String url = conf.get(DBConfiguration.URL_PROPERTY);
			try {
				DriverManager.getDriver(url);

			} catch (SQLException e) {

				if (driverShim == null) {

					if (driver == null) {
						ClassLoader classLoader = conf.getClassLoader();
						@SuppressWarnings("unchecked")
						Class<? extends Driver> driverClass = (Class<? extends Driver>) classLoader
								.loadClass(conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
						driver = driverClass.newInstance();

					}

					driverShim = new JdbcDriverShim(driver);
					DriverManager.registerDriver(driverShim);
					LOG.debug("[CrateInputFormat] Registered JDBC driver via shim {}. Actual Driver {}.", driverShim, driver);
				}
			}

			if (conf.get(DBConfiguration.USERNAME_PROPERTY) == null) {
				connection = DriverManager.getConnection(url);

			} else {
				connection = DriverManager.getConnection(url, conf.get(DBConfiguration.USERNAME_PROPERTY),
						conf.get(DBConfiguration.PASSWORD_PROPERTY));
			}

			connection.setAutoCommit(false);

		} catch (Exception e) {
			throw Throwables.propagate(e);
		}

		return connection;

	}

	/*
	 * Versions > HDP-2.3.4 started using createConnection 
	 * instead of getConnection
	 */
	public Connection createConnection() {
		return getConnection();
	}

	@Override
	protected RecordReader<LongWritable, T> createDBRecordReader(DBInputSplit split, Configuration conf) throws IOException {
		final RecordReader<LongWritable, T> dbRecordReader = super.createDBRecordReader(split, conf);

		return new RecordReader<LongWritable, T>() {
			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
				dbRecordReader.initialize(split, context);
			}

			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				return dbRecordReader.nextKeyValue();
			}

			@Override
			public LongWritable getCurrentKey() throws IOException, InterruptedException {
				return dbRecordReader.getCurrentKey();
			}

			@Override
			public T getCurrentValue() throws IOException, InterruptedException {
				return dbRecordReader.getCurrentValue();
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return dbRecordReader.getProgress();
			}

			@Override
			public void close() throws IOException {
				dbRecordReader.close();
				try {
					DriverManager.deregisterDriver(driverShim);
				} catch (SQLException e) {
					throw new IOException(e);
				}
			}
		};
	}

	@Override
	protected void closeConnection() {
		super.closeConnection();
		try {
			DriverManager.deregisterDriver(driverShim);
		} catch (SQLException e) {
			throw Throwables.propagate(e);
		}
	}

	public static void setInput(Configuration hadoopConf, Class<? extends DBWritable> inputClass, String countQuery, String inputQuery) {

		DBConfiguration dbConf = new DBConfiguration(hadoopConf);

		dbConf.setInputClass(inputClass);
		/*
		 * [CrateSource] is based on the 'older' DBInputFormat which computes
		 * the splits from a count query; this approach requires the provisioning
		 * of a count query
		 */
		dbConf.setInputCountQuery(countQuery);
		dbConf.setInputQuery(inputQuery);
		
		
		
	}
}
