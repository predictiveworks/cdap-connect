package de.kp.works.connect.common.jdbc;
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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import org.apache.hadoop.io.NullWritable;

import java.sql.*;
import java.util.*;

public abstract class JdbcSink<V extends JdbcWritable> extends BatchSink<StructuredRecord, NullWritable, V> {

	protected Class<? extends Driver> driverClass;

	protected abstract String getJdbcPluginID();

	protected abstract String getJdbcPluginName();

	protected abstract String getJdbcPluginType();

	protected abstract String getJdbcDriverName();

	protected abstract String getEndpoint();

	protected abstract Properties getProperties();

	protected abstract String getTableName();

	@Override
	public void initialize(BatchRuntimeContext context) throws Exception {
		super.initialize(context);
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		super.configurePipeline(pipelineConfigurer);
		/*
		 * Validate that the JDBC plugin class is available; it is loaded in
		 * 'initialize' and should be available here
		 */
		String pluginId = getJdbcPluginID();

		String pluginName = getJdbcPluginName();
		String pluginType = getJdbcPluginType();

		try {

			Class<? extends Driver> jdbcDriverClass = pipelineConfigurer.usePluginClass(pluginType, pluginName,
					pluginId, PluginProperties.builder().build());

			if (jdbcDriverClass == null)
				throw new IllegalArgumentException(
						String.format("Unable to load JDBC Driver class for plugin name '%s'.", pluginId));

		} catch (Exception e) {
			throw new IllegalArgumentException(
					String.format("Unable to load JDBC Driver class for plugin name '%s' with: %s.", pluginId,
							e.getLocalizedMessage()));
		}

	}
	
	protected void registerJdbcDriver(BatchSinkContext context) throws Exception {

		String jdbcPluginId = getJdbcPluginID();
		Class<? extends Driver> driverClass = context.loadPluginClass(jdbcPluginId);

		JdbcDriverShim driverShim = new JdbcDriverShim(driverClass.newInstance());
		DriverManager.registerDriver(driverShim);

	}
	
	protected Schema getSchema(BatchSinkContext context) throws Exception {

		Schema schema = context.getInputSchema();
		if (schema != null) {

			/*
			 * In case of an existing input schema, we validate whether 
			 * this schema is compliant with the specified table.
			 */
			if (!validateSchema(schema))
				throw new Exception(String.format("[%s] Provided schema is not compliant with the specified table columns.", JdbcSink.class.getName()));

		} else {
			/*
			 * This stage does not provide any input schema. We therefore infer the schema 
			 * from the specified database connection. The respective schema may still be
			 * null, because the specified table does not exist yet.
			 * 
			 * In this case, we dynamically create the table from the provided record schema
			 * dynamically. For more details, see CrateWritable
			 */
			schema = inferSchema();
		}

		return schema;
		
	}
	
	protected Boolean validateSchema(Schema inputSchema) throws Exception {

		Connection connection = null;
		Statement statement = null;

		try {

			connection = DriverManager.getConnection(getEndpoint(), getProperties());

			/* Check if table already exists */
			if (!tableExists(connection, getTableName())) {
				/*
				 * The specified table does not exist; in this case, 
				 * validation returns TRUE as the table is dynamically
				 * created
				 */
				connection.close();
				return true;

			}

			statement = connection.createStatement();

			String sql = String.format("SELECT * FROM %s WHERE 1 = 0", getTableName());
			ResultSet rs = statement.executeQuery(sql);

			ResultSetMetaData rsMetaData = rs.getMetaData();

			/* Validate fields */

			Set<String> invalidFields = new HashSet<>();

			assert inputSchema.getFields() != null;
			for (Schema.Field field : inputSchema.getFields()) {

				int columnIndex = rs.findColumn(field.getName());
				if (!isValidField(field, rsMetaData, columnIndex)) {
					invalidFields.add(field.getName());
				}
			}

			statement.close();
			connection.close();

			return invalidFields.isEmpty();

		} catch (Exception e) {

			if (statement != null)
				statement.close();

			if (connection != null)
				connection.close();

			return false;

		}

	}

	/*
	 * This method expacts that the Jdbc Driver is registered and available
	 */
	protected Schema inferSchema() throws Exception {

		List<Schema.Field> fields = new ArrayList<>();

		Connection connection = null;
		Statement statement = null;

		try {

			connection = DriverManager.getConnection(getEndpoint(), getProperties());

			/* Check if table already exists */
			if (!tableExists(connection, getTableName())) {
				connection.close();
				return null;

			}

			statement = connection.createStatement();

			String sql = String.format("SELECT * FROM %s WHERE 1 = 0", getTableName());
			ResultSet rs = statement.executeQuery(sql);

			fields.addAll(JdbcUtils.getSchemaFields(rs));
			Schema schema = Schema.recordOf("jdbcSchema", fields);

			statement.close();
			connection.close();

			return schema;

		} catch (Exception e) {

			if (statement != null)
				statement.close();

			if (connection != null)
				connection.close();

			return null;

		}
	}

	protected Boolean tableExists(Connection conn, String table) throws SQLException {

		DatabaseMetaData metadata = conn.getMetaData();

		ResultSet rs = metadata.getTables(null, null, table, null);
		return (rs.next());

	}

	/*
	 * Checks if field of the input schema is compatible with corresponding database
	 * column.
	 */
	protected boolean isValidField(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {
		/*
		 * STEP #1: Check nullable compatibility
		 */
		boolean isColumnNullable = (ResultSetMetaData.columnNullable == metadata.isNullable(index));
		boolean isNotNullAssignable = !isColumnNullable && field.getSchema().isNullable();

		if (isNotNullAssignable)
			return false;

		/*
		 * STEP #2: Check schema type compatibility
		 */
		Schema inSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();

		int sqlType = metadata.getColumnType(index);
		int precision = metadata.getPrecision(index);

		int scale = metadata.getScale(index);
		boolean signed = metadata.isSigned(index);
		
		Schema outSchema = Schema.of(JdbcUtils.getSchemaType(sqlType, precision, scale, signed));

		if (!Objects.equals(inSchema.getType(), outSchema.getType())
				|| !Objects.equals(inSchema.getLogicalType(), outSchema.getLogicalType())) {
			return false;
		}

		return true;
	}

}
