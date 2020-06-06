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

import com.orientechnologies.orient.core.metadata.schema.OType;
import io.cdap.cdap.api.data.schema.Schema;

public class OrientUtil {

	public static final String ORIENT_URL = "orient.url";

	public static final String ORIENT_USER = "orient.user";
	public static final String ORIENT_PASSWORD = "orient.password";

	public static final String ORIENT_EDGE_TYPE = "orient.edge.type";
	public static final String ORIENT_VERTEX_TYPE = "orient.vertex.type";

	public static final String ORIENT_WRITE = "orient.write";

	public static OType toOrientType(Schema schema) {

		Schema.Type schemaType = (schema.isNullable()) ? schema.getNonNullable().getType() : schema.getType();

		switch (schemaType) {
		/** BASIC DATA TYPES **/
		case BOOLEAN:
			return OType.BOOLEAN;
		case DOUBLE:
			return OType.DOUBLE;
		case ENUM:
			return OType.STRING;
		case FLOAT:
			return OType.FLOAT;
		case INT:
			return OType.INTEGER;
		case LONG:
			return OType.LONG;
		case STRING:
			return OType.STRING;
		/** COMPLEX DATA TYPES **/
		case BYTES:
			return OType.BINARY;
		case ARRAY:
			return OType.EMBEDDEDLIST;
		case MAP:
			return OType.EMBEDDEDMAP;
		default:
			throw new IllegalArgumentException(String.format("Schema type '%s' not supported.", schemaType.name()));
		}

	}
}
