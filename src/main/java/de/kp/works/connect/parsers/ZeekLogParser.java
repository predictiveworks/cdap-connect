package de.kp.works.connect.parsers;
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


import java.io.Serializable;
import java.util.Map.Entry;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import de.kp.works.connect.core.SchemaUtil;
/**
 * TODO Parse a list of messages
 */
public class ZeekLogParser implements Serializable {

	private static final long serialVersionUID = 8308295289540439271L;
	/*
	 * The identifier of the respective Zeek log message
	 */
	private String key;
	/*
	 * The associated CDAP schema
	 */
	private Schema schema;
	/*
	 * The payload that is used to build a structured 
	 * record
	 */
	private JsonObject payload;
	
	public void toJson(String message) throws Exception {
		/*
		 * STEP #1: Parse [String] message
		 */
		JsonParser parser = new JsonParser();
		JsonElement jsonElement = parser.parse(message);
		
		if (!jsonElement.isJsonObject())
			throw new Exception(String.format("[%s] Zeek log messages must be Json objects.", this.getClass().getName()));
		
		JsonObject jsonObject = jsonElement.getAsJsonObject();
		JsonObject cleanObject = new JsonObject();
		
		for (Entry<String,JsonElement> entry : jsonObject.entrySet()) {
			
			String entryKey = entry.getKey();
			JsonElement entryValue = entry.getValue();
			
			String cleanKey = entryKey.replaceAll("[^\\._a-zA-Z0-9]+","");
			cleanObject.add(cleanKey, entryValue);
			
		}
		/*
		 * STEP #2: Extract key of log message
		 * and associated payload
		 */
		if (cleanObject.get("type") == null) {
			/*
			 * This is a nested Zeek (Bro) log message
			 */
			Entry<String, JsonElement> entry = cleanObject.entrySet().iterator().next();
			key = entry.getKey();
			
			if (key == null)
				throw new Exception(String.format("[%s] Zeek log format is unknown and cannot be processed.", this.getClass().getName()));
			
			payload = entry.getValue().getAsJsonObject();
			
		} else {
			/*
			 * This is a plain Zeek (Bro) log message
			 */
			key = cleanObject.get("type").getAsString();
			payload = cleanObject;
		}
		/*
		 * STEP #3: Rename and transform timestamp field
		 */
		replaceName("timestamp", "ts");
		if (payload.get("timestamp") != null) {
		      
			long timestamp = 0L;
			try {
				
				Double ts = payload.get("timestamp").getAsDouble();
				timestamp = (long) (ts * 1000);
				
			} catch (Exception e) {
				;
			}

			payload.remove("timestamp");
			payload.addProperty("timestamp", timestamp);
			
		}
		/*
		 * STEP #4: Harmonization of field names
		 */
		replaceName("ip_src_host", "source_ip");
		replaceName("ip_src_host", "id.orig_h");
		replaceName("ip_src_host", "tx_hosts");
		
		replaceName("ip_dst_host", "dest_ip");
		replaceName("ip_dst_host", "id.resp_h");
		replaceName("ip_dst_host", "rx_hosts");

		replaceName("ip_src_port", "source_port");
		replaceName("ip_src_port", "id.orig_p");

		replaceName("ip_dst_port", "dest_port");
		replaceName("ip_dst_port", "id.resp_p");
		
		payload.addProperty("protocol", key);

	}
	
	public void toJson(byte[] message) throws Exception {	      
		String text = new String(message, "UTF-8");
		toJson(text);		
	}
	
	public StructuredRecord toRecord(String message) throws Exception {
		toJson(message);
		return toRecord();
	}
	
	public StructuredRecord toRecord(byte[] message) throws Exception {
		toJson(message);
		return toRecord();
	}	
	
	public String getKey() {
		return key;
	}
	
	public JsonObject getPayload() {
		return payload;
	}
	
	private StructuredRecord toRecord() throws Exception {
		
		/* Infer schema */
		schema = SchemaUtil.inferSchema(payload);

		/* Retrieve structured record */
		String json = payload.toString();
		
		StructuredRecord record = StructuredRecordStringConverter.fromJsonString(json, schema);
		return record;

	}
	
	private void replaceName(String newName, String oldName) {
		
		if (payload == null || payload.get(oldName) == null) return;
		JsonElement value = payload.remove(oldName);
		
		payload.add(newName, value);
		
	}
	
	public static void main(String[] args) {
		
		String httpMessage = "{\"http\": {\"ts\":1467657279,\"uid\":\"CMYLzP3PKiwZAgBa51\",\"id.orig_h\":\"192.168.138.158\",\"id.orig_p\":49206,\"id.resp_h\":\"95.163.121.204\"," +
				"\"id.resp_p\":80,\"trans_depth\":2,\"method\":\"GET\",\"host\":\"7oqnsnzwwnm6zb7y.gigapaysun.com\",\"uri\":\"/img/flags/it.png\",\"referrer\":\"http://7oqnsnzwwnm6zb7y.gigapaysun.com/11iQmfg\",\"user_agent\":\"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0)\",\"request_body_len\":0,\"response_body_len\":552,\"status_code\":200,\"status_msg\":\"OK\",\"tags\":[],\"resp_fuids\":[\"F3m7vB2RjUe4n01aqj\"],\"resp_mime_types\":[\"image/png\"]}}";

		
		// String dnsMessage = "{\"ts\":1449511228.474, \"uid\":\"CFgSLp4HgsGqXnNjZi\", \"id.orig_h\":\"104.130.172.191\", \"id.orig_p\":33893, \"id.resp_h\":\"69.20.0.164\", \"id.resp_p\":53, \"proto\":\"udp\", \"trans_id\":3514, \"rcode\":3, \"rcode_name\":\"NXDOMAIN\", \"AA\":false, \"TC\":false, \"RD\":false, \"RA\":false, \"Z\":0, \"rejected\":false, \"sensor\":\"cloudbro\", \"type\":\"dns\" }";
		
		try {
			
			ZeekLogParser parser = new ZeekLogParser();
			System.out.println(parser.toRecord(httpMessage).toString());
			
			System.out.println(parser.getPayload());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
