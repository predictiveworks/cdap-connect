package de.kp.works.connect.iot.thingsboard;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

public class ThingsboardClient {

	private static final Logger log = LoggerFactory.getLogger(ThingsboardClient.class);

	private final ThingsboardSinkConfig config;

	private Map<String, JsonObject> assetMap;

	private CloseableHttpClient httpClient;
	private RequestConfig httpConfig;
	/*
	 * The JWT token is retrieved via the login request, that replaces
	 * user name & password by an access token for subsequent requests
	 */
	private String token;
	private int timeout = 30;

	public ThingsboardClient(ThingsboardSinkConfig config) {

		this.config = config;

		this.httpConfig = RequestConfig.custom().setConnectTimeout(timeout * 1000).setSocketTimeout(timeout * 1000)
				.setConnectionRequestTimeout(timeout * 1000).build();

		try {
			login();
			initialize();

		} catch (Exception e) {
			log.error("Initializing Thingsboard Client failed '{}'", e.getLocalizedMessage());
		}

	}

	public void sendTelemetryToAsset(StructuredRecord record) throws Exception {

		String assetName = (String) record.get(this.config.assetName);
		String assetType = this.config.assetType;

		if (StringUtils.isEmpty(assetName)) {
			return;
		}

		JsonObject jsonAsset = getOrCreateAsset(assetName, assetType);		
		String assetId = jsonAsset.get("id").getAsJsonObject().get("id").getAsString();

		List<Map<String, Object>> data = getData(record);

		try {

			this.httpClient = HttpClientBuilder.create().setDefaultRequestConfig(httpConfig).build();
			/*
			 * Endpoint to pass telemetry data to Thingsboard server
			 */
			String endpoint = this.config.getEndpoint()
					+ String.format("/api/plugins/telemetry/ASSET/%s/timeseries/values", assetId);
			
			HttpPost post = new HttpPost(endpoint);
			post.setHeader("Content-Type", "application/json; charset=UTF-8");
			post.setHeader("X-Authorization", "Bearer " + token);

			ObjectMapper mapper = new ObjectMapper();
			ObjectWriter writer = mapper.writerFor(new TypeReference<List<Map<String, Object>>>() {
			});

			post.setEntity(new ByteArrayEntity(writer.writeValueAsBytes(data)));
			httpClient.execute(post);

		} finally {
			if (this.httpClient != null) {
				this.httpClient.close();
			}
			this.httpClient = null;
		}

	}

	private List<Map<String,Object>> getData(StructuredRecord record) throws Exception {

		List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();

		List<String> columns = new ArrayList<String>(Arrays.asList(this.config.assetFeatures.split(",")));
		Schema schema = record.getSchema();
		/*
		 * This approach is restricted to numeric fields
		 */
		for (Schema.Field field : schema.getFields()) {

			String fname = field.getName();
			Object fvalu = null;

			if (columns.contains(fname) == false ) continue;

			Schema fschema = field.getSchema();

			switch (fschema.getType()) {
			case ARRAY:
				throw new Exception("[ERROR] ARRAY is not supported");
			case BOOLEAN:
				throw new Exception("[ERROR] BOOLEAN is not supported");
			case BYTES:
				throw new Exception("[ERROR] BYTES is not supported");
			case DOUBLE:
				fvalu = (Double) record.get(fname);
			case ENUM:
				throw new Exception("[ERROR] ENUM is not supported");
			case FLOAT:
				fvalu = (Float) record.get(fname);
			case INT:
				fvalu = (Integer) record.get(fname);
			case LONG:
				fvalu = (Long) record.get(fname);
			case NULL:
				throw new Exception("[ERROR] NULL is not supported");
			case MAP:
				throw new Exception("[ERROR] ARRAY is not supported");
			case RECORD:
				throw new Exception("[ERROR] RECORD is not supported");
			case STRING:
				throw new Exception("[ERROR] STRING is not supported");
			case UNION:
				throw new Exception("[ERROR] UNION is not supported");
			}

			Map<String, Object> entry = new HashMap<String, Object>();
			entry.put(fname, fvalu);

			data.add(entry);

		}
		
		return data;
		
	}
	/*
	 * curl -X POST 
	 * 
	 * --header 'Content-Type: application/json' 
	 * --header 'Accept: application/json' 
	 * -d '{"username":"tenant@thingsboard.org", "password":"tenant"}' 
	 * 'http://THINGSBOARD_URL/api/auth/login'
	 *
	 */
	private void login() throws Exception {

		try {
			/*
			 * Build Http client to retrieve access token from login endpoint
			 */
			this.httpClient = HttpClientBuilder.create().setDefaultRequestConfig(httpConfig).build();

			String endpoint = this.config.getEndpoint() + "/api/auth/login";
			HttpPost post = new HttpPost(endpoint);

			post.setHeader("Content-Type", "application/json; charset=UTF-8");
			post.setHeader("Accept", "application/json");

			JsonObject credentials = new JsonObject();
			credentials.addProperty("username", this.config.user);
			credentials.addProperty("password", this.config.password);

			post.setEntity(new ByteArrayEntity(credentials.toString().getBytes("UTF-8")));
			CloseableHttpResponse response = httpClient.execute(post);

			try {

				HttpEntity entity = response.getEntity();
				if (entity != null) {

					String body = EntityUtils.toString(entity);
					EntityUtils.consume(entity);
					/*
					 * {"token":"$YOUR_JWT_TOKEN", "refreshToken":"$YOUR_JWT_REFRESH_TOKEN"}
					 */
					JsonObject node = new JsonParser().parse(body).getAsJsonObject();
					this.token = node.get("token").getAsString();

				} else
					throw new Exception("Thingsboard server returns empty response");

			} finally {
				if (response != null) {
					response.close();
				}
			}

		} finally {
			if (this.httpClient != null) {
				this.httpClient.close();
			}
			this.httpClient = null;
		}

	}

	private void initialize() throws Exception {

		String assetLimit = this.config.assetLimit;
		String assetType = this.config.assetType;

		assetMap = new HashMap<>();

		try {

			this.httpClient = HttpClientBuilder.create().setDefaultRequestConfig(httpConfig).build();

			String endpoint = this.config.getEndpoint()
					+ String.format("/api/tenant/assets?limit=%s&type=%s", assetLimit, assetType);
			HttpGet get = new HttpGet(endpoint);

			get.setHeader("Content-Type", "application/json; charset=UTF-8");
			get.setHeader("X-Authorization", "Bearer " + token);

			CloseableHttpResponse response = httpClient.execute(get);
			try {

				HttpEntity entity = response.getEntity();
				if (entity != null) {

					String body = EntityUtils.toString(entity);
					EntityUtils.consume(entity);

					JsonObject result = new JsonParser().parse(body).getAsJsonObject();
					JsonArray assets = result.get("data").getAsJsonArray();
					
					Iterator<JsonElement> iter = assets.iterator();
					while (iter.hasNext()) {
						
						JsonObject jsonAsset = iter.next().getAsJsonObject();
						String name = jsonAsset.get("name").getAsString();
						
						assetMap.put(name, jsonAsset);
					}

				} else
					throw new Exception("Thingsboard server returns empty response");

			} finally {
				if (response != null) {
					response.close();
				}
			}

		} finally {
			if (this.httpClient != null) {
				this.httpClient.close();
			}
			this.httpClient = null;
		}

	}

	private JsonObject getOrCreateAsset(String assetName, String assetType) throws Exception {
		
		JsonObject asset = assetMap.get(assetName);
		if (asset == null) {
			asset = createAsset(assetName, assetType);
			assetMap.put(assetName, asset);
		}
		return asset;
	}
	/*
	 * This method is used to represent a ThingsBoard
	 * [Asset] as a JsonObject to avoid unneccessary
	 * dependencies
	 */
	private JsonObject buildJsonAsset(String assetName, String assetType) {
		
		JsonObject jAsset = new JsonObject();
		
		jAsset.add("id", JsonNull.INSTANCE);
		jAsset.addProperty("createdTime", 0);
		
		jAsset.add("additionalInfo", JsonNull.INSTANCE);
		jAsset.add("tenantId", JsonNull.INSTANCE);
		
		jAsset.add("customerId", JsonNull.INSTANCE);
		
		jAsset.addProperty("name", assetName);
		jAsset.addProperty("type", assetType);

		return jAsset;
		
	}
	
	private JsonObject createAsset(String assetName, String assetType) throws Exception {

		JsonObject responseAsset = null;

		try {

			this.httpClient = HttpClientBuilder.create().setDefaultRequestConfig(httpConfig).build();

			JsonObject sendAsset = buildJsonAsset(assetName, assetType);

			/* Set header */
			String endpoint = this.config.getAssetEndpoint();
			HttpPost post = new HttpPost(endpoint);

			post.setHeader("Content-Type", "application/json; charset=UTF-8");
			post.setHeader("X-Authorization", "Bearer " + token);

			/* Set asset */
			post.setEntity(new ByteArrayEntity(sendAsset.toString().getBytes("UTF-8")));
			CloseableHttpResponse response = httpClient.execute(post);

			try {

				HttpEntity entity = response.getEntity();
				if (entity != null) {

					String body = EntityUtils.toString(entity);
					EntityUtils.consume(entity);

				    responseAsset = new JsonParser().parse(body).getAsJsonObject();

				} else
					throw new Exception("Thingsboard server returns empty response");

			} finally {
				if (response != null) {
					response.close();
				}
			}

		} finally {
			if (this.httpClient != null) {
				this.httpClient.close();
			}
			this.httpClient = null;
		}

		return responseAsset;

	}

}
