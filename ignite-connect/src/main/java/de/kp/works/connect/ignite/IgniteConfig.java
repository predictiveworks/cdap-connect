package de.kp.works.connect.ignite;
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

import com.google.common.base.Strings;
import de.kp.works.connect.common.BaseConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;

import javax.annotation.Nullable;
import javax.cache.Cache;
import java.nio.ByteBuffer;
import java.util.*;

public class IgniteConfig extends BaseConfig {

	private static final long serialVersionUID = 2258065855745759770L;

	protected static final String SSL_VERIFY_DESC = "An indicator to determine whether certificates have to be verified. "
			+ "Supported values are 'true' and 'false'. If 'false', untrusted trust "
			+ "certificates (e.g. self signed), will not lead to an error.";

	protected static final String CIPHER_SUITES_DESC = "A comma-separated list of cipher suites which are allowed for "
			+ "a secure connection. Samples are TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_RSA_WITH_AES_128_GCM_SHA256 and others.";

	protected static final String KEYSTORE_ALGO_DESC = "The algorithm used for the client SSL keystore.";

	protected static final String KEYSTORE_PASS_DESC = "The password of the client SSL keystore.";

	protected static final String KEYSTORE_PATH_DESC = "A path to a file which contains the client SSL keystore.";

	protected static final String KEYSTORE_TYPE_DESC = "The format of the client SSL keystore. Supported values are 'JKS', "
			+ "'JCEKS' and 'PKCS12'.";

	protected static final String TRUSTSTORE_PATH_DESC = "A path to a file which contains the client SSL truststore.";

	protected static final String TRUSTSTORE_TYPE_DESC = "The format of the client SSL truststore. Supported values are 'JKS', "
			+ "'JCEKS' and 'PKCS12'.";

	protected static final String TRUSTSTORE_ALGO_DESC = "The algorithm used for the client SSL truststore.";

	protected static final String TRUSTSTORE_PASS_DESC = "The password of the client SSL truststore.";

	@Description("The host of the Apache Ignite cluster.")
	@Macro
	public String host;

	@Description("The port of the Apache Ignite cluster.")
	@Macro
	public Integer port;

	@Description("The name of the Apache Ignite cache used to organize data.")
	@Macro
	public String cacheName;

	/*** CREDENTIALS ***/

	@Description("Name of a registered user name. Required for authentication.")
	@Macro
	@Nullable
	public String user;

	@Description("Password of the registered user. Required for authentication.")
	@Macro
	@Nullable
	public String password;
	/*
	 * TLS SECURITY
	 */
	@Description("Indicator to determine whether SSL transport security is used or not.")
	@Macro
	public String sslMode;

	@Description(SSL_VERIFY_DESC)
	@Macro
	@Nullable
	public String sslVerify;

	@Description(CIPHER_SUITES_DESC)
	@Macro
	@Nullable
	public String sslCipherSuites;

	@Description(KEYSTORE_PATH_DESC)
	@Macro
	@Nullable
	public String sslKeyStorePath;

	@Description(KEYSTORE_TYPE_DESC)
	@Macro
	@Nullable
	public String sslKeyStoreType;

	@Description(KEYSTORE_ALGO_DESC)
	@Macro
	@Nullable
	public String sslKeyStoreAlgo;

	@Description(KEYSTORE_PASS_DESC)
	@Macro
	@Nullable
	public String sslKeyStorePass;

	@Description(TRUSTSTORE_PATH_DESC)
	@Macro
	@Nullable
	public String sslTrustStorePath;

	@Description(TRUSTSTORE_TYPE_DESC)
	@Macro
	@Nullable
	public String sslTrustStoreType;

	@Description(TRUSTSTORE_ALGO_DESC)
	@Macro
	@Nullable
	public String sslTrustStoreAlgo;

	@Description(TRUSTSTORE_PASS_DESC)
	@Macro
	@Nullable
	public String sslTrustStorePass;

	public IgniteConfig() {
	}
	
	public Properties getConfig() {
		
		Properties config = new Properties();
		
		config.setProperty(IgniteUtil.IGNITE_HOST, host);
		config.setProperty(IgniteUtil.IGNITE_PORT, String.valueOf(port));
		
		config.setProperty(IgniteUtil.IGNITE_CACHE_NAME, cacheName);

		if (!Strings.isNullOrEmpty(IgniteUtil.IGNITE_USER))
			config.setProperty(IgniteUtil.IGNITE_USER, user);

		if (!Strings.isNullOrEmpty(IgniteUtil.IGNITE_PASSWORD))
			config.setProperty(IgniteUtil.IGNITE_PASSWORD, password);
		
		/* SSL CONFIGURATION */
		
		config.setProperty(IgniteUtil.IGNITE_SSL_MODE, sslMode);

		if (!Strings.isNullOrEmpty(IgniteUtil.IGNITE_SSL_VERIFY))
			config.setProperty(IgniteUtil.IGNITE_SSL_VERIFY, sslVerify);

		if (!Strings.isNullOrEmpty(IgniteUtil.IGNITE_SSL_CIPHER_SUITES))
			config.setProperty(IgniteUtil.IGNITE_SSL_CIPHER_SUITES, sslCipherSuites);

		if (!Strings.isNullOrEmpty(IgniteUtil.IGNITE_SSL_KEYSTORE_PATH))
			config.setProperty(IgniteUtil.IGNITE_SSL_KEYSTORE_PATH, sslKeyStorePath);

		if (!Strings.isNullOrEmpty(IgniteUtil.IGNITE_SSL_KEYSTORE_TYPE))
			config.setProperty(IgniteUtil.IGNITE_SSL_KEYSTORE_TYPE, sslKeyStoreType);

		if (!Strings.isNullOrEmpty(IgniteUtil.IGNITE_SSL_KEYSTORE_PASS))
			config.setProperty(IgniteUtil.IGNITE_SSL_KEYSTORE_PASS, sslKeyStorePass);

		if (!Strings.isNullOrEmpty(IgniteUtil.IGNITE_SSL_KEYSTORE_ALGO))
			config.setProperty(IgniteUtil.IGNITE_SSL_KEYSTORE_ALGO, sslKeyStoreAlgo);

		if (!Strings.isNullOrEmpty(IgniteUtil.IGNITE_SSL_TRUSTSTORE_PATH))
			config.setProperty(IgniteUtil.IGNITE_SSL_TRUSTSTORE_PATH, sslTrustStorePath);

		if (!Strings.isNullOrEmpty(IgniteUtil.IGNITE_SSL_TRUSTSTORE_TYPE))
			config.setProperty(IgniteUtil.IGNITE_SSL_TRUSTSTORE_TYPE, sslTrustStoreType);

		if (!Strings.isNullOrEmpty(IgniteUtil.IGNITE_SSL_TRUSTSTORE_PASS))
			config.setProperty(IgniteUtil.IGNITE_SSL_TRUSTSTORE_PASS, sslTrustStorePass);

		if (!Strings.isNullOrEmpty(IgniteUtil.IGNITE_SSL_TRUSTSTORE_ALGO))
			config.setProperty(IgniteUtil.IGNITE_SSL_TRUSTSTORE_ALGO, sslTrustStoreAlgo);
		
		return config;
		
	}
		
	public void validate() {
		super.validate();

		if (Strings.isNullOrEmpty(host)) {
			throw new IllegalArgumentException(
					String.format("[%s] The cluster host must not be empty.", this.getClass().getName()));
		}

		if (port < 1) {
			throw new IllegalArgumentException(
					String.format("[%s] The cluster port must be positive.", this.getClass().getName()));
		}

		if (Strings.isNullOrEmpty(cacheName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The cache name must not be empty.", this.getClass().getName()));
		}
		
	}
	
	public Schema getSchema(String fieldNames) {

		Schema schema;
		List<Schema.Field> fields = new ArrayList<>();
		
		try {
			/*
			 * We expect here that the Apache Ignite 
			 * context is initiated properly already
			 */
			IgniteClient ignite = IgniteContext.getInstance().getClient();
			ClientCache<String, org.apache.ignite.binary.BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();
			/*
			 * STEP #2: Build ScanQuery
			 */
			ScanQuery<String, org.apache.ignite.binary.BinaryObject> scan = new ScanQuery<>();
			scan.setPageSize(10);

			List<Cache.Entry<String,org.apache.ignite.binary.BinaryObject>> entries = cache.query(scan).getAll();
			if (entries.size() == 0)
				throw new IllegalArgumentException(String.format("The cache '%s' provided has no entries", cacheName));
						
			org.apache.ignite.binary.BinaryObject binaryObject = entries.get(0).getValue();
			for (String fieldName: Objects.requireNonNull(IgniteUtil.string2Array(fieldNames))) {
				Object fieldValue = binaryObject.field(fieldName);
				fields.add(Schema.Field.of(fieldName, value2Schema(fieldValue)));
			}
			
			schema = Schema.recordOf("ignite", fields);

		} catch (Exception e) {
			schema = Schema.recordOf("ignite", fields);

		}

		return schema;

	}

	public Object datetime2Long(Object value) {
		
		/*
		 * Java Date, Time & Timestamp are mapped
		 * into a LONG type 
		 */
		
		if (value instanceof java.sql.Date)
			return ((java.sql.Date)value).getTime();
		
		else if (value instanceof java.sql.Time)
			return ((java.sql.Time)value).getTime();
		
		else if (value instanceof java.sql.Timestamp)
			return ((java.sql.Timestamp)value).getTime();
		
		else
			return value;
		
	}
	
	public Object fromFieldValue(Object value, Schema schema) {

		switch (schema.getType()) {

			/* BASIC DATA TYPES */
			case DOUBLE:
			case FLOAT:
			case INT:
			case STRING:
				return value;
			/*
			 * Java Date, Time & Timestamp are mapped
			 * into a LONG type
			 */
			case LONG:
				return datetime2Long(value);

			/* COMPLEX DATA TYPES */
			case BYTES:
				return ByteBuffer.wrap((byte[]) value);

			case ARRAY: {

				/* value must be collection */
				Collection<?> collection = (Collection<?>) value;
				List<Object> result = new ArrayList<>(collection.size());

				for (Object element : collection) {
					/*
					 * Nullable is not relevant here: see schema inference
					 */
					assert schema.getComponentSchema() != null;
					result.add(fromFieldValue(element, schema.getComponentSchema()));

				}

				return result;
			}
			case MAP: {

				/* value mus be a map */
				Map<?, ?> map = (Map<?, ?>) value;
				Map<Object, Object> result = new LinkedHashMap<>(map.size());

				Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();

				assert mapSchema != null;
				Schema keySchema = mapSchema.getKey();
				Schema valueSchema = mapSchema.getValue();

				for (Map.Entry<?, ?> entry : map.entrySet()) {
					result.put(fromFieldValue(entry.getKey(), keySchema), fromFieldValue(entry.getValue(), valueSchema));
				}

				return result;

			}
			default:
				throw new IllegalArgumentException(
						String.format("Data type '%s' is not supported.", schema.getType().name()));
			}

	}

	/*
	 * A helper method to infer the data type from the
	 * field value of an Ignite BinaryObject 
	 */
	public Schema value2Schema(Object value) {

		Schema schema = null;

		/* BASIC TYPES */
		if (value instanceof Boolean) 
			schema = Schema.of(Schema.Type.BOOLEAN);

		else if (value instanceof Double)
			schema = Schema.of(Schema.Type.DOUBLE);

		else if (value instanceof Float)
			schema = Schema.of(Schema.Type.FLOAT);

		else if (value instanceof Integer)
			schema = Schema.of(Schema.Type.INT);

		else if (value instanceof Long)
			schema = Schema.of(Schema.Type.LONG);
		/*
		 * A [Short] type should not occur, referring to Ignite's data type
		 * documentation
		 */
		else if (value instanceof Short)
			schema = Schema.of(Schema.Type.LONG);

		else if (value instanceof String)
			schema = Schema.of(Schema.Type.STRING);

		/* DATE & TIME TYPES */
		
		/*
		 * Java Date, Time & Timestamp are mapped
		 * into a LONG type 
		 */
		
		else if (value instanceof java.sql.Date)
			schema = Schema.of(Schema.Type.LONG);
		
		else if (value instanceof java.sql.Time)
			schema = Schema.of(Schema.Type.LONG);
		
		else if (value instanceof java.sql.Timestamp)
			schema = Schema.of(Schema.Type.LONG);
		
		
		/* COMPLEX DATA TYPES */
		else if (value instanceof Collection<?>) {

			List<Object> values = new ArrayList<>((Collection<?>) value);
			Object head = values.get(0);
			/*
			 * Special treatment of Byte Arrays
			 */
			if (head instanceof Byte) {
				schema = Schema.of(Schema.Type.BYTES);

			} else {
				schema = Schema.arrayOf(value2Schema(head));

			}

		} else if (value instanceof Map<?, ?>) {
			/*
			 * The support of MAP is left here, even if we have
			 * no evidence that Ignite caches are able to cope
			 * with maps
			 */
			Map<Object, Object> map = new HashMap<>((Map<?, ?>) value);
			Map.Entry<Object, Object> head = map.entrySet().iterator().next();

			schema = Schema.mapOf(value2Schema(head.getKey()), value2Schema(head.getValue()));

		} else if (value.getClass().isArray()) {

			List<Object> values = new ArrayList<>(Arrays.asList((Object[]) value));
			Object head = values.get(0);
			/*
			 * Special treatment of Byte Arrays
			 */
			if (head instanceof Byte) {
				schema = Schema.of(Schema.Type.BYTES);

			} else {
				schema = Schema.arrayOf(value2Schema(head));

			}

		}

		return schema;

	}

}
