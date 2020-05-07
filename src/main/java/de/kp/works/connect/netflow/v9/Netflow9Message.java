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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import de.kp.works.connect.netflow.NetflowMessage;
import de.kp.works.connect.netflow.NetflowMode;

public class Netflow9Message extends NetflowMessage {

	private static final String FIELD_FLOW_KIND = "flow_kind";
	private static final String FIELD_FLOW_RECORD_COUNT = "flow_count";
	 
	 
	private static final String FIELD_RECIPIENT = "recipient";
	
	private static final String FIELD_SENDER = "sender";
	private static final String FIELD_SEQUENCE_NUMBER = "packet_seq";

	private static final String FIELD_SOURCE_ID = "source_id";
	private static final String FIELD_SOURCE_ID_RAW = "source_id_raw";
	
	private static final String FIELD_SYS_UPTIME_MS = "uptime";
	private static final String FIELD_FLOW_TEMPLATE_ID = "flow_template_id";
	
	private static final String FIELD_UNIX_SECONDS = "seconds";
	private static final String FIELD_VERSION = "version";

	private NetflowMode mode;
	private final List<Netflow9Field> fields = new ArrayList<>();
	
	private JsonObject message = new JsonObject();

	public void setNetflowMode(NetflowMode mode) {
		this.mode = mode;
	}

	public void setFlowCount(Integer value) {
		message.addProperty(FIELD_FLOW_RECORD_COUNT, value);
	}

	public void setFlowFields(List<Netflow9Field> value) {
		
		if (value != null)
			fields.addAll(value);

		
	    switch (mode) {
	      case RAW: {
	    	  
	    	  	for (Netflow9Field field : fields) {
	    	  		
	    	  		String fieldName = field.getFieldName();
	    	  		byte[] fieldValue = field.getRawValue();

	    	  		JsonElement jsonElement = new Gson().toJsonTree(fieldValue);
	    			message.add(fieldName, jsonElement);
	    	  		
	    	  	}

	    	  	break;
	      
	      }
	      case INTERPRETED: {
	    	  
	    	  	for (Netflow9Field field : fields) {
	    	  		
	    	  		String fieldName = field.getFieldName();
	    	  		Object fieldValue = field.getValue();
	    	  		
	    	  		if (fieldValue == null)
	    	  			message.add(fieldName, JsonNull.INSTANCE);
	    	  		
	    	  		else
	    	  			message.add(fieldName, new Gson().toJsonTree(fieldValue));
	    	  		
	    	  	}

	    	  	break;
	      
	      }
	    }
	
	}

	public void setFlowKind(FlowKind value) {
		message.addProperty(FIELD_FLOW_KIND, value.name().toLowerCase());
	}

	public void setFlowTemplateId(Integer value) {
		message.addProperty(FIELD_FLOW_TEMPLATE_ID, value);
	}

	public void setSender(InetSocketAddress value) {
		message.addProperty(FIELD_SENDER, (value == null) ? null : value.toString());
	}

	public void setPacketSeq(Long value) {
		message.addProperty(FIELD_SEQUENCE_NUMBER, value);
	}

	public void setRecipient(InetSocketAddress value) {
		message.addProperty(FIELD_RECIPIENT, (value == null) ? null : value.toString());
	}

	public void setSourceId(Long value) {
		message.addProperty(FIELD_SOURCE_ID, value);
	}

	public void setSourceIdRaw(byte[] value) {
		
		JsonElement jsonElement = new Gson().toJsonTree(value);
		message.add(FIELD_SOURCE_ID_RAW, jsonElement);

	}

	public void setUnixSeconds(Long value) {
		message.addProperty(FIELD_UNIX_SECONDS, value);
	}

	public void setUptime(Long value) {
		message.addProperty(FIELD_SYS_UPTIME_MS, value);
	}

	public void setVersion(Integer value) {
		message.addProperty(FIELD_VERSION, value);
	}

	@Override
	public StructuredRecord toRecord() throws Exception {

		/* Retrieve structured record */
		String json = message.toString();
		return StructuredRecordStringConverter.fromJsonString(json, getSchema());

	}

	private Schema getSchema() {

		List<Schema.Field> schemaFields = new ArrayList<>();

		schemaFields.add(Schema.Field.of(FIELD_FLOW_KIND, Schema.of(Schema.Type.STRING)));

		schemaFields.add(Schema.Field.of(FIELD_SENDER, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		schemaFields.add(Schema.Field.of(FIELD_RECIPIENT, Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/***** HEADER *****/
		
		schemaFields.add(Schema.Field.of(FIELD_VERSION, Schema.of(Schema.Type.INT)));
		schemaFields.add(Schema.Field.of(FIELD_FLOW_RECORD_COUNT, Schema.of(Schema.Type.INT)));

		schemaFields.add(Schema.Field.of(FIELD_SYS_UPTIME_MS, Schema.of(Schema.Type.LONG)));
		schemaFields.add(Schema.Field.of(FIELD_UNIX_SECONDS, Schema.of(Schema.Type.LONG)));

		schemaFields.add(Schema.Field.of(FIELD_SEQUENCE_NUMBER, Schema.of(Schema.Type.LONG)));

		schemaFields.add(Schema.Field.of(FIELD_SOURCE_ID, Schema.of(Schema.Type.LONG)));
		schemaFields.add(Schema.Field.of(FIELD_SOURCE_ID_RAW, Schema.of(Schema.Type.BYTES)));
		
		/***** DATA *****/
		
		schemaFields.add(Schema.Field.of(FIELD_FLOW_TEMPLATE_ID, Schema.of(Schema.Type.INT)));
		
	    switch (mode) {
	      case RAW: {
	    	  
	    	  	for (Netflow9Field field : fields) {
	    	  		
	    	  		String fieldName = field.getFieldName();
	    	  		Schema fieldSchema = Schema.of(Schema.Type.BYTES);
	    	  		
	    			schemaFields.add(Schema.Field.of(fieldName, fieldSchema));
	    	  		
	    	  	}

	    	  	break;
	      
	      }
	      case INTERPRETED: {
	    	  
	    	  	for (Netflow9Field field : fields) {
	    	  		
	    	  		String fieldName = field.getFieldName();
	    	  		Schema fieldSchema = field.getSchema();
	    	  		
	    			schemaFields.add(Schema.Field.of(fieldName, fieldSchema));
	    	  		
	    	  	}

	    	  	break;
	      
	      }
	    }

		Schema schema = Schema.recordOf("netflow9Schema", schemaFields);
		return schema;

	}
}
