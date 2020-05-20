package de.kp.works.connect.jdbc.crate;
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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.kp.works.connect.jdbc.JdbcOutputFormat;

public class CrateOutputFormat<K, V extends CrateWritable> extends JdbcOutputFormat<K, V> {

	private static final Logger LOG = LoggerFactory.getLogger(CrateOutputFormat.class);

	public String insertQuery(String table, String primaryKey, String[] fieldNames) {

		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ").append(table);
		/*
		 * Append column block
		 */
		sb.append(" (");
		for (int i = 0; i < fieldNames.length; i++) {
			sb.append(fieldNames[i]);
			if (i != fieldNames.length - 1) {
				sb.append(",");
			}
		}
		sb.append(")");
		/*
		 * Append binding block
		 */
		sb.append(" VALUES (");

		for (int i = 0; i < fieldNames.length; i++) {
			sb.append("?");
			if (i != fieldNames.length - 1) {
				sb.append(",");
			}
		}

		sb.append(")");
		/*
		 * Append duplicate block; it is important to note, that the KEY must be
		 * excluded from this block
		 */
		sb.append(" ON DUPLICATE KEY UPDATE ");
		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(primaryKey))
				continue;
			sb.append(fieldNames[i] + "=VALUES(" + fieldNames[i] + ")");
			if (i != fieldNames.length - 1) {
				sb.append(",");
			}
		}
		/*
		 * We have to omit the ';' at the end
		 */
		return sb.toString();

	}

	public class CrateRecordWriter extends RecordWriter<K, V> {

		private Connection connection;
		private PreparedStatement statement;

		private boolean emptyData = true;

		public CrateRecordWriter() throws SQLException {
		}

		/*
		 * The prepared statement can empty, if the input schema is not provided in the
		 * initial phase of this stage
		 */
		public CrateRecordWriter(Connection connection, PreparedStatement statement) throws SQLException {

			this.connection = connection;
			this.statement = statement;

			this.connection.setAutoCommit(false);

		}

		public Connection getConnection() {
			return connection;
		}

		public PreparedStatement getStatement() {
			return statement;
		}

		/*
		 * Implementation of the close method below is the exact implementation in
		 * DBOutputFormat except that we check if there is any data to be written and if
		 * not, we skip executeBatch call.
		 * 
		 * There might be reducers that don't receive any data and thus this check is
		 * necessary to prevent empty data to be committed.
		 */
		@Override
		public void close(TaskAttemptContext context) throws IOException {

			/*
			 * We expect that the statement is available at this processing phase
			 */
			Connection connection = getConnection();
			PreparedStatement statement = getStatement();

			try {

				if (!emptyData) {

					if (statement == null)
						throw new SQLException("[CrateOutputFormat] PreparedStatement is null.");

					statement.executeBatch();
					connection.commit();

				}

			} catch (SQLException e) {
				try {
					connection.rollback();

				} catch (SQLException ex) {
					LOG.warn(StringUtils.stringifyException(ex));

				}
				throw new IOException(e.getMessage());

			} finally {
				try {

					if (statement != null)
						statement.close();
					connection.close();

				} catch (SQLException ex) {
					throw new IOException(ex.getMessage());
				}
			}

			try {
				DriverManager.deregisterDriver(driverShim);

			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			try {
				/*
				 * There may be the necessity to create the output table dynamically, i.e. from
				 * the schema of the provided records
				 */
				statement = value.write(connection, statement);
				statement.addBatch();

				emptyData = false;

			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		/*
		 * The configuration has been provided by the Crate output format provider,
		 * which uses DBConfiguration properties to specify configuration parameters;
		 * therefore [DBConfiguration] is used to extract them
		 */
		Configuration conf = context.getConfiguration();
		/*
		 * The primary key of the table is important, as this [CrateSink] supports
		 * JDBC's DUPLICATE ON KEY feature to enable proper update requests in case of
		 * key conflicts.
		 * 
		 * The property 'mapreduce.jdbc.primaryKey' is an internal provided property
		 */
		String primaryKey = conf.get("mapreduce.jdbc.primaryKey");
		String tableName = conf.get(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY);

		/*
		 * In case there is no schema available, the list of field names is empty; this
		 * indicates that neither an input schema is provided nor schema inference
		 * worked.
		 */
		String[] fieldNames = new String[] {};
		if (conf.get(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY) != null) {
			fieldNames = conf.get(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY).split(",");
		}

		try {

			Connection connection = getConnection(conf);
			PreparedStatement statement = (fieldNames == null) ? null
					: connection.prepareStatement(insertQuery(tableName, primaryKey, fieldNames));

			return new CrateRecordWriter(connection, statement);

		} catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
	}

}
