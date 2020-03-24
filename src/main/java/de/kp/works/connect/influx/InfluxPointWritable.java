package de.kp.works.connect.influx;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.core.SchemaUtil;

public class InfluxPointWritable implements Writable, InfluxWritable, Configurable {

	private StructuredRecord record;

	private String measurement;
	private String timefield;

	private Configuration conf;

	public InfluxPointWritable(StructuredRecord record, String measurement, String timefield) {
		this.record = record;
		this.measurement = measurement;
	}

	/**
	 * Used in map-reduce. Do not remove.
	 */
	public InfluxPointWritable() {
	}

	private static Schema getNonNullIfNullable(Schema schema) {
		return schema.isNullable() ? schema.getNonNullable() : schema;
	}

	private static Double toDouble(Schema.Type fieldType, Object fieldValue) throws Exception {

		switch (fieldType) {

		case DOUBLE: {
			return (Double) fieldValue;
		}
		case FLOAT: {
			return Double.valueOf((Float) fieldValue);

		}
		case INT: {
			return Double.valueOf((Integer) fieldValue);
		}
		case LONG: {
			return Double.valueOf((Long) fieldValue);
		}
		default:
			throw new Exception("Field type is not numeric.");
		}

	}

	private static Boolean isNumeric(Schema.Type fieldType) {

		switch (fieldType) {
		case ARRAY:
		case BOOLEAN:
		case BYTES:
		case MAP:
		case NULL:
		case RECORD:
		case ENUM:
		case STRING:
		case UNION:
			return false;
		case DOUBLE:
		case FLOAT:
		case INT:
		case LONG:
			return true;

		default:
			return false;
		}

	}

	private static Boolean isString(Schema.Type fieldType) {

		if (!fieldType.equals(Schema.Type.STRING))
			return false;
		return true;

	}

	public void write(InfluxDB influxDB) {

		Schema schema = record.getSchema();

		Point.Builder builder = Point.measurement(measurement);
		if (timefield == null) {
			builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

			/* Append all numeric field within the data record */
			List<Schema.Field> fields = schema.getFields();
			for (Schema.Field field : fields) {

				String fieldName = field.getName();
				Schema.Type fieldType = getNonNullIfNullable(field.getSchema()).getType();

				if (isNumeric(fieldType)) {

					try {
						builder.addField(fieldName, toDouble(fieldType, record.get(fieldName)));
					} catch (Exception e) {
						;
					}

				} else if (isString(fieldType)) {
					builder.tag(fieldName, (String) record.get(fieldName));

				} else
					continue;

			}

		} else {

			Schema.Field timeField = schema.getField(timefield);
			if (timeField == null) {
				throw new IllegalArgumentException(String.format(
						"[%s] The data record does not contain the specified time field.", this.getClass().getName()));
			}

			Schema.Type timeType = getNonNullIfNullable(timeField.getSchema()).getType();
			if (SchemaUtil.isTimeType(timeType) == false) {
				throw new IllegalArgumentException("The data type of the time field must be LONG.");
			}

			Long time = (Long) record.get(timefield);
			builder.time(time, TimeUnit.MILLISECONDS);
			/*
			 * Append all numeric field within the data record
			 */
			List<Schema.Field> fields = schema.getFields();
			for (Schema.Field field : fields) {

				String fieldName = field.getName();

				if (fieldName.equals(timefield))
					continue;

				Schema.Type fieldType = getNonNullIfNullable(field.getSchema()).getType();
				if (isNumeric(fieldType)) {

					try {
						builder.addField(fieldName, toDouble(fieldType, record.get(fieldName)));
					} catch (Exception e) {
						;
					}

				} else if (isString(fieldType)) {
					builder.tag(fieldName, (String) record.get(fieldName));

				} else
					continue;

			}

		}

		Point point = builder.build();
		influxDB.write(point);

	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		throw new IOException(
				String.format("[%s] Method 'readFields' from DataInput is not implemented", this.getClass().getName()));
	}

	@Override
	public void write(DataOutput output) throws IOException {
		throw new IOException(
				String.format("[%s] Method 'write' from DataOutput is not implemented", this.getClass().getName()));
	}

}
