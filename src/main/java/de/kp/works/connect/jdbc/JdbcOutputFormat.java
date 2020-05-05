package de.kp.works.connect.jdbc;
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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.gson.Gson;

import de.kp.works.connect.jdbc.JdbcDriverShim;

public abstract class JdbcOutputFormat<K, V extends JdbcWritable> extends OutputFormat<K, V> {

	private static final Logger LOG = LoggerFactory.getLogger(JdbcOutputFormat.class);

	private static Gson GSON = new Gson();

	protected Driver driver;
	protected JdbcDriverShim driverShim;

	private static String URL_PROPERTY = DBConfiguration.URL_PROPERTY;
	private static String DRIVER_CLASS_PROPERTY = DBConfiguration.DRIVER_CLASS_PROPERTY;

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		/*
		 * The current implementation does not need any output checks
		 */
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		/*
		 * This implementation of the output committer is a copy of the respective
		 * method in DBOutputFormat
		 */
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
	}

	/*
	 * This is an internal method to retrieve the Jdbc database connection from the
	 * provided configuration
	 */
	public Connection getConnection(Configuration conf) {

		Connection connection;
		try {

			String url = conf.get(URL_PROPERTY);
			try {
				DriverManager.getDriver(url);

			} catch (SQLException e) {

				if (driverShim == null) {

					if (driver == null) {
						ClassLoader classLoader = conf.getClassLoader();
						@SuppressWarnings("unchecked")
						Class<? extends Driver> driverClass = (Class<? extends Driver>) classLoader
								.loadClass(conf.get(DRIVER_CLASS_PROPERTY));
						driver = driverClass.newInstance();

					}

					driverShim = new JdbcDriverShim(driver);
					DriverManager.registerDriver(driverShim);

					LOG.debug(String.format("[%s] Registered JDBC driver via shim %s. Actual Driver %s.",
							JdbcOutputFormat.class.getName(), driverShim, driver));
				}
			}

			String json = conf.get("jdbc.properties");
			Properties properties = GSON.fromJson(json, Properties.class);

			connection = DriverManager.getConnection(url, properties);
			connection.setAutoCommit(false);

		} catch (Exception e) {
			throw Throwables.propagate(e);
		}

		return connection;

	}

}
