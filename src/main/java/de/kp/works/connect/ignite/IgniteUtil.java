package de.kp.works.connect.ignite;
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

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Strings;

public class IgniteUtil {

	public static final String IGNITE_HOST = "ignite.host";
	public static final String IGNITE_PORT = "ignite.port";

	public static final String IGNITE_USER = "ignite.user";
	public static final String IGNITE_PASSWORD = "ignite.password";

	public static final String IGNITE_CACHE_NAME = "ignite.cache";
	public static final String IGNITE_FIELDS = "ignite.fields";
	public static final String IGNITE_PARTITIONS = "ignite.partitions";
	
	/** SSL SUPPORT **/
	
	public static final String IGNITE_SSL_MODE = "ignite.ssl.mode";
	public static final String IGNITE_SSL_VERIFY = "ignite.ssl.verify";

	public static final String IGNITE_SSL_CIPHER_SUITES = "ignite.ssl.cipher.suites";
	
	public static final String IGNITE_SSL_KEYSTORE_PATH = "ignite.ssl.keystore.path";
	public static final String IGNITE_SSL_KEYSTORE_TYPE = "ignite.ssl.keystore.type";
	public static final String IGNITE_SSL_KEYSTORE_PASS = "ignite.ssl.keystore.password";	
	public static final String IGNITE_SSL_KEYSTORE_ALGO = "ignite.ssl.keystore.algorithm";

	public static final String IGNITE_SSL_TRUSTSTORE_PATH = "ignite.ssl.truststore.path";
	public static final String IGNITE_SSL_TRUSTSTORE_TYPE = "ignite.ssl.truststore.type";
	public static final String IGNITE_SSL_TRUSTSTORE_PASS = "ignite.ssl.truststore.password";	
	public static final String IGNITE_SSL_TRUSTSTORE_ALGO = "ignite.ssl.truststore.algorithm";

	public static String getHost(Configuration conf) {
		return conf.get(IGNITE_HOST);
	}

	public static void setHost(Configuration conf, String host) {
		conf.set(IGNITE_HOST, host);
	}

	public static String getPort(Configuration conf) {
		return conf.get(IGNITE_PORT);
	}

	public static void setPort(Configuration conf, String port) {
		conf.set(IGNITE_PORT, port);
	}

	public static String getUser(Configuration conf) {
		return conf.get(IGNITE_USER);
	}

	public static void setUser(Configuration conf, String user) {
		conf.set(IGNITE_USER, user);
	}

	public static String getPass(Configuration conf) {
		return conf.get(IGNITE_PASSWORD);
	}

	public static void setPass(Configuration conf, String password) {
		conf.set(IGNITE_PASSWORD, password);
	}
	
	public static String getCacheName(Configuration conf) {
		return conf.get(IGNITE_CACHE_NAME);
	}

	public static void setCacheName(Configuration conf, String cacheName) {
		conf.set(IGNITE_CACHE_NAME, cacheName);
	}
	
	public static String[] getFields(Configuration conf) {

		String fields = conf.get(IGNITE_FIELDS);
		if (Strings.isNullOrEmpty(fields))
			return null;

		else {
			return string2Array(fields);
		}
	
	}

	public static void setFields(Configuration conf, String fields) {
		conf.set(IGNITE_FIELDS, fields);
	}
	
	public static int getPartitions(Configuration conf) {
		return Integer.valueOf(conf.get(IGNITE_PARTITIONS));
	}

	public static void setPartitions(Configuration conf, int partitions) {
		conf.set(IGNITE_PARTITIONS, String.valueOf(partitions));
	}
	
	public static String getSslMode(Configuration conf) {
		return conf.get(IGNITE_SSL_MODE);
	}

	public static void setSslMode(Configuration conf, String sslMode) {
		conf.set(IGNITE_SSL_MODE, sslMode);
	}
	
	public static String getSslVerify(Configuration conf) {
		return conf.get(IGNITE_SSL_VERIFY);
	}

	public static void setSslVerify(Configuration conf, String sslVerify) {
		conf.set(IGNITE_SSL_VERIFY, sslVerify);
	}
	
	/** KEY STORE **/
	
	public static String[] getCipherSuites(Configuration conf) {

		String cipherSuites = conf.get(IGNITE_SSL_CIPHER_SUITES);
		if (Strings.isNullOrEmpty(cipherSuites))
			return null;

		else {
			return string2Array(cipherSuites);
		}
	
	}

	public static void setCipherSuites(Configuration conf, String cipherSuites) {
		conf.set(IGNITE_SSL_CIPHER_SUITES, cipherSuites);
	}
	
	public static String getSslKeystorePath(Configuration conf) {
		return conf.get(IGNITE_SSL_KEYSTORE_PATH);
	}

	public static void setSslKeystorePath(Configuration conf, String sslKeystorePath) {
		conf.set(IGNITE_SSL_KEYSTORE_PATH, sslKeystorePath);
	}
	
	public static String getSslKeystoreType(Configuration conf) {
		return conf.get(IGNITE_SSL_KEYSTORE_TYPE);
	}

	public static void setSslKeystoreType(Configuration conf, String sslKeystoreType) {
		conf.set(IGNITE_SSL_KEYSTORE_TYPE, sslKeystoreType);
	}
	
	public static String getSsKeystorePass(Configuration conf) {
		return conf.get(IGNITE_SSL_TRUSTSTORE_PASS);
	}

	public static void setSslKeystorePass(Configuration conf, String sslKeystorePass) {
		conf.set(IGNITE_SSL_KEYSTORE_PASS, sslKeystorePass);
	}
	
	public static String getSslKeystoreAlgo(Configuration conf) {
		return conf.get(IGNITE_SSL_KEYSTORE_ALGO);
	}

	public static void setSslKeystoreAlgo(Configuration conf, String sslKeystoreAlgo) {
		conf.set(IGNITE_SSL_KEYSTORE_ALGO, sslKeystoreAlgo);
	}
	
	/** TRUST STORE **/
	
	public static String getSslTruststorePath(Configuration conf) {
		return conf.get(IGNITE_SSL_TRUSTSTORE_PATH);
	}

	public static void setSslTruststorePath(Configuration conf, String sslTruststorePath) {
		conf.set(IGNITE_SSL_TRUSTSTORE_PATH, sslTruststorePath);
	}
	
	public static String getSslTruststoreType(Configuration conf) {
		return conf.get(IGNITE_SSL_TRUSTSTORE_TYPE);
	}

	public static void setSslTruststoreType(Configuration conf, String sslTruststoreType) {
		conf.set(IGNITE_SSL_TRUSTSTORE_TYPE, sslTruststoreType);
	}
	
	public static String getSslTruststorePass(Configuration conf) {
		return conf.get(IGNITE_SSL_TRUSTSTORE_PASS);
	}

	public static void setSslTruststorePass(Configuration conf, String sslTruststorePass) {
		conf.set(IGNITE_SSL_TRUSTSTORE_PASS, sslTruststorePass);
	}
	
	public static String getSslTruststoreAlgo(Configuration conf) {
		return conf.get(IGNITE_SSL_TRUSTSTORE_ALGO);
	}

	public static void setSslTruststoreAlgo(Configuration conf, String sslTruststoreAlgo) {
		conf.set(IGNITE_SSL_TRUSTSTORE_ALGO, sslTruststoreAlgo);
	}
		
	public static String[] string2Array(String value) {

		if (Strings.isNullOrEmpty(value))
			return null;

		else {
			
			String[] tokens = value.split(",");
			String[] result = new String[tokens.length];
			
			for (int i = 0; i < tokens.length; i++) {
				result[i] = tokens[i].trim();
			}
			
			return result;
		}
	
	}

}
