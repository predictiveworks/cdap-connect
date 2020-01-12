package de.kp.works.connect.thingsboard;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import org.thingsboard.server.common.data.asset.Asset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

public class ThingsboardClient {

	private static final Logger log = LoggerFactory.getLogger(ThingsboardClient.class);

	private final ThingsboardConfig config;

	private Map<String, Asset> assetMap;

	private CloseableHttpClient httpClient;
	private RequestConfig httpConfig;

	private String token;
	private int timeout = 30;

	public ThingsboardClient(ThingsboardConfig config) {

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

		Asset asset = getOrCreateAsset(assetName, assetType);		
		UUID assetId = asset.getId().getId();

		List<Map<String, Object>> data = getData(record);

		try {

			this.httpClient = HttpClientBuilder.create().setDefaultRequestConfig(httpConfig).build();
			/*
			 * Endpoint to pass telemetry data to Thingsboard server
			 */
			String endpoint = this.config.getEndpoint()
					+ String.format("/api/plugins/telemetry/ASSET/%s/timeseries/values", assetId.toString());
			
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
	
	private void login() throws Exception {

		try {

			this.httpClient = HttpClientBuilder.create().setDefaultRequestConfig(httpConfig).build();

			String endpoint = this.config.getEndpoint() + "/api/auth/login";
			HttpPost post = new HttpPost(endpoint);

			post.setHeader("Content-Type", "application/json; charset=UTF-8");
			post.setHeader("X-Authorization", "Bearer " + token);

			Map<String, String> credentials = new HashMap<String, String>();
			credentials.put("username", this.config.user);
			credentials.put("password", this.config.password);

			ObjectMapper mapper = new ObjectMapper();

			ObjectReader reader = mapper.readerFor(JsonNode.class);
			ObjectWriter writer = mapper.writerFor(Map.class);

			post.setEntity(new ByteArrayEntity(writer.writeValueAsBytes(credentials)));
			CloseableHttpResponse response = httpClient.execute(post);

			try {

				HttpEntity entity = response.getEntity();
				if (entity != null) {

					String body = EntityUtils.toString(entity);
					EntityUtils.consume(entity);

					JsonNode node = reader.readValue(body);
					this.token = node.get("token").asText();

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

					ObjectMapper mapper = new ObjectMapper();
					ObjectReader reader = mapper.readerFor(JsonNode.class);

					JsonNode node = reader.readValue(body);
					JsonNode data = node.findValues("data").get(0);

					reader = mapper.readerFor(new TypeReference<List<Asset>>() {
					});

					List<Asset> assets = reader.readValue(data);
					assets.stream().forEach(a -> assetMap.put(a.getName(), a));

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

	private Asset getOrCreateAsset(String assetName, String assetType) throws Exception {
		Asset asset = assetMap.get(assetName);
		if (asset == null) {
			asset = createAsset(assetName, assetType);
			assetMap.put(assetName, asset);
		}
		return asset;
	}

	private Asset createAsset(String assetName, String assetType) throws Exception {

		Asset responseAsset = null;

		try {

			this.httpClient = HttpClientBuilder.create().setDefaultRequestConfig(httpConfig).build();

			Asset sendAsset = new Asset();

			sendAsset.setName(assetName);
			sendAsset.setType(assetType);

			/* Set header */
			String endpoint = this.config.getAssetEndpoint();
			HttpPost post = new HttpPost(endpoint);

			post.setHeader("Content-Type", "application/json; charset=UTF-8");
			post.setHeader("X-Authorization", "Bearer " + token);

			/* Set asset */
			ObjectMapper mapper = new ObjectMapper();
			ObjectWriter writer = mapper.writerFor(Asset.class);

			post.setEntity(new ByteArrayEntity(writer.writeValueAsBytes(sendAsset)));
			CloseableHttpResponse response = httpClient.execute(post);

			try {

				HttpEntity entity = response.getEntity();
				if (entity != null) {

					String body = EntityUtils.toString(entity);
					EntityUtils.consume(entity);

					ObjectReader reader = mapper.readerFor(Asset.class);
					responseAsset = reader.readValue(body);

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
