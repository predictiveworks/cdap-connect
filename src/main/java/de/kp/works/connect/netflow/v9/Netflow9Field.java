package de.kp.works.connect.netflow.v9;
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

import co.cask.cdap.api.data.schema.Schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.google.common.base.Charsets;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.Ints;

public class Netflow9Field {

	public static final int SCOPE_FIELD_OFFSET = 1000;
	
	private Schema schema;

	private String name;
	private Object value;

	private byte[] bytes;
	
	private Netflow9FieldTemplate fieldTemplate;

	public Netflow9Field(Netflow9FieldTemplate fieldTemplate) {
		this.fieldTemplate = fieldTemplate;
	}

	public void setBigInteger(byte[] value, Schema.Type type) {

		this.bytes = value;
		
		final BigInteger bigInt = new BigInteger(1, value);
		BigDecimal bigDecimal = new BigDecimal(bigInt);

		switch (type) {
		case INT: {

			this.schema = Schema.of(Schema.Type.INT);
			this.name = getFieldName();

			this.value = bigDecimal.intValue();

		}
		case LONG: {

			this.schema = Schema.of(Schema.Type.LONG);
			this.name = getFieldName();

			this.value = bigDecimal.longValue();

		}
		default: {
			this.schema = Schema.of(Schema.Type.DOUBLE);
			this.name = getFieldName();

			this.value = bigDecimal.doubleValue();

		}
		}

	}

	public void setByte(byte[] value) throws Exception {

		this.bytes = value;

		this.schema = Schema.nullableOf(Schema.of(Schema.Type.INT));
		this.name = getFieldName();

		if (value.length != 1)
			this.value = null;

		else {
			/*
			 * Adapted from https://stackoverflow.com/a/4266881/375670
			 * 
			 * return the unsigned equivalent of a single Java byte (signed) as an int
			 */
			int intVal = value[0] & 0xFF;
			this.value = intVal;
		}
	}

	public void setBytes(byte[] value) {

		this.bytes = value;

		this.schema = Schema.of(Schema.Type.BYTES);
		this.name = getFieldName();

		this.value = value;

	}

	public void setIpV4Address(byte[] value) {

		this.bytes = value;

		this.schema = Schema.nullableOf(Schema.of(Schema.Type.STRING));
		this.name = getFieldName();
		
	    try {
	    	
	      this.value = InetAddress.getByAddress(value).getHostAddress();

	    } catch (UnknownHostException e) {
	      this.value = null;
	    }
	  }
	
	public void setIPV6Address(byte[] value) {

		this.bytes = value;

		this.schema = Schema.nullableOf(Schema.of(Schema.Type.STRING));
		this.name = getFieldName();

		try {

			this.value = Inet6Address.getByAddress(value).getHostAddress();

		} catch (UnknownHostException e) {
			this.value = null;

		}

	}

	public void setLong(byte[] value) {

		this.bytes = value;

		this.schema = Schema.nullableOf(Schema.of(Schema.Type.LONG));
		this.name = getFieldName();

		if (value.length != 4)
			this.value = null;

		else {
			/*
			 * Adapted from https://stackoverflow.com/a/1576404/375670
			 */
			long longVal = Ints.fromByteArray(value) & 0xFFFFFFFFL;
			this.value = longVal;

		}

	}

	public void setMacAddress(byte[] value) {

		this.bytes = value;

		this.schema = Schema.of(Schema.Type.STRING);
		this.name = getFieldName();

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < value.length; i++) {
			sb.append(String.format("%02X", value[i]));
			if (i < value.length - 1) {
				sb.append(":");
			}
		}

		this.value = sb.toString();

	}

	/*
	 * Adapted from https://stackoverflow.com/a/7932774/375670
	 * 
	 * value are the raw bytes to interpret as an unsigned short and returned as an
	 * INTEGER field with the unsigned short value
	 */
	public void setShort(byte[] value) throws Exception {

		this.bytes = value;

		this.schema = Schema.nullableOf(Schema.of(Schema.Type.INT));
		this.name = getFieldName();

		if (value.length != 2)
			this.value = null;

		else {
			final short shortVal = Shorts.fromByteArray(value);
			int intVal = shortVal >= 0 ? shortVal : 0x10000 + shortVal;

			this.value = intVal;
		}
	}

	public void setString(byte[] value) {

		this.bytes = value;

		/*
		 * The character for these fields is not discussed in any known documentation;
		 * we use UTF-8 (which handles single byte ASCII encoding transparently) until
		 * proven inadequate
		 */

		this.schema = Schema.of(Schema.Type.STRING);
		this.name = getFieldName();

		this.value = new String(value, Charsets.UTF_8);

	}

	public String getName() {
		return name;
	}

	public Schema getSchema() {
		return schema;
	}

	public Object getValue() {
		return value;
	}

	public byte[] getRawValue() {
		return bytes;
	}
	
	public String getFieldName() {

		if (fieldTemplate.getType() != null) {
			return fieldTemplate.getType().name();

		} else {
			return String.format("type_%d", fieldTemplate.getTypeId());

		}
	}

}
