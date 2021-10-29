package de.kp.works.connect.saphana;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import de.kp.works.connect.common.jdbc.JdbcOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SAPHanaOutputFormat<K, V extends SAPHanaWritable> extends JdbcOutputFormat<K, V> {

	private static final Logger LOG = LoggerFactory.getLogger(SAPHanaOutputFormat.class);

	/*
	 * This method defines an upsert query statement without
	 * a trailing semicolon;
	 */
	public String upsertQuery(String table, String primaryKey, String[] fieldNames) {
		/*
		 * We need to insert new rows and update existing ones;
		 * therefore SAP's UPSERT command is used: more details
		 * 
		 * https://help.sap.com/viewer/4fe29514fd584807ac9f2a04f6754767/2.0.03/en-US/20fc06a7751910149892c0d09be21a38.html
		 * 
		 * Field names (other than the table names) are not escaped
		 * in UPSERT statements
		 */
		StringBuilder sb = new StringBuilder();
		sb.append("UPSERT ").append(table);
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
		sb.append(" WITH PRIMARY KEY");
		/*
		 * We have to omit the ';' at the end
		 */
		return sb.toString();

	}

	public class SAPHanaRecordWriter extends RecordWriter<K, V> {

		private Connection connection;
		private PreparedStatement statement;

		private boolean emptyData = true;

		public SAPHanaRecordWriter() {
		}

		/*
		 * The prepared statement can empty, if the input schema is not provided in the
		 * initial phase of this stage
		 */
		public SAPHanaRecordWriter(Connection connection, PreparedStatement statement) throws SQLException {

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
						throw new SQLException("[SAPHanaOutputFormat] PreparedStatement is null.");

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
					LOG.warn(StringUtils.stringifyException(ex));
				}
			}

			try {
				DriverManager.deregisterDriver(driverShim);

			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		@Override
		public void write(K key, V value) {
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
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {
		/*
		 * The configuration has been provided by the SAP Hana output format provider,
		 * which uses DBConfiguration properties to specify configuration parameters;
		 * therefore [DBConfiguration] is used to extract them
		 */
		Configuration conf = context.getConfiguration();

		/* The table name is escaped here */
		String tableName = conf.get(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY);

		/*
		 * In case there is no schema available, the list of field names is empty; this
		 * indicates that neither an input schema is provided nor schema inference
		 * worked.
		 */
		String primaryKey = conf.get("mapreduce.jdbc.primaryKey");
		
		String[] fieldNames = new String[] {};
		if (conf.get(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY) != null) {
			fieldNames = conf.get(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY).split(",");
		}

		try {

			Connection connection = getConnection(conf);
			PreparedStatement statement = (fieldNames == null) ? null
					: connection.prepareStatement(upsertQuery(tableName, primaryKey, fieldNames));

			return new SAPHanaRecordWriter(connection, statement);

		} catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
	}

}
