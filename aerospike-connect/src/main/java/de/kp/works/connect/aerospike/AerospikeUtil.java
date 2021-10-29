package de.kp.works.connect.aerospike;
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

public class AerospikeUtil {

	/** COMMON **/
	
	public static final String AEROSPIKE_HOST = "aerospike.host";
	public static final String AEROSPIKE_PORT = "aerospike.port";

	public static final String AEROSPIKE_NAMESPACE = "aerospike.namespace";
	public static final String AEROSPIKE_SET = "aerospike.set";

	public static final String AEROSPIKE_TIMEOUT = "aerospike.timeout";

	public static final String AEROSPIKE_USER = "aerospike.user";
	public static final String AEROSPIKE_PASSWORD = "aerospike.password";

	/** READ / WRITE **/
	
	public static final String AEROSPIKE_BINS = "aerospike.bins";
	
	public static final String AEROSPIKE_EXPIRATION = "aerospike.expiration";

	public static final String AEROSPIKE_WRITE = "aerospike.write";

	public static final String AEROSPIKE_OPERATION = "aerospike.operation";
	public static final String DEFAULT_OPERATION = "scan";
	
	public static final String AEROSPIKE_NUMRANGE_BIN = "aerospike.numrange.bin";
	public static final String AEROSPIKE_NUMRANGE_BEGIN = "aerospike.numrange.begin";
	public static final String AEROSPIKE_NUMRANGE_END = "aerospike.numrange.end";
	
	public static final long INVALID_LONG = 762492121482318889L;

	public static String[] getBins(Configuration conf) {

		String bins = conf.get(AEROSPIKE_BINS);
		if (bins == null || bins.equals(""))
			return null;

		else
			return bins.split(",");
	
	}

	public static void setBins(Configuration conf, String bins) {
		conf.set(AEROSPIKE_BINS, bins);
	}

	public static String getHost(Configuration conf) {
		return conf.get(AEROSPIKE_HOST);
	}

	public static void setHost(Configuration conf, String host) {
		conf.set(AEROSPIKE_HOST, host);
	}

	public static String getNamespace(Configuration conf) {
		return conf.get(AEROSPIKE_NAMESPACE);
	}

	public static void setNamespace(Configuration conf, String namespace) {
		conf.set(AEROSPIKE_NAMESPACE, namespace);
	}

	public static String getNumRangeBin(Configuration conf) {
		return conf.get(AEROSPIKE_NUMRANGE_BIN, "");
	}

	public static void setNumRangeBin(Configuration conf, String binname) {
		conf.set(AEROSPIKE_NUMRANGE_BIN, binname);
	}

	public static long getNumRangeBegin(Configuration conf) {

		long begin = conf.getLong(AEROSPIKE_NUMRANGE_BEGIN, INVALID_LONG);
		if (begin == INVALID_LONG)
			throw new UnsupportedOperationException("Missing input numrange begin");

		return begin;

	}

	public static void setNumRangeBegin(Configuration conf, long begin) {
		conf.setLong(AEROSPIKE_NUMRANGE_BEGIN, begin);
	}

	public static long getNumRangeEnd(Configuration conf) {

		long end = conf.getLong(AEROSPIKE_NUMRANGE_END, INVALID_LONG);
		if (end == INVALID_LONG )
			throw new UnsupportedOperationException("Missing input numrange end");

		return end;
	
	}

	public static void setNumRangeEnd(Configuration conf, long end) {
		conf.setLong(AEROSPIKE_NUMRANGE_END, end);
	}
	
	public static String getOperation(Configuration conf) {

		String operation = conf.get(AEROSPIKE_OPERATION, DEFAULT_OPERATION);
		if (!operation.equals("scan") && !operation.equals("numrange"))
			throw new UnsupportedOperationException("Input operation must be 'scan' or 'numrange'");

		return operation;
	
	}

	public static void setOperation(Configuration conf, String operation) {
		
		if (!operation.equals("scan") && !operation.equals("numrange"))
			throw new UnsupportedOperationException("Input operation must be 'scan' or 'numrange'");

		conf.set(AEROSPIKE_OPERATION, operation);
	
	}

	public static int getPort(Configuration conf) {
		return conf.getInt(AEROSPIKE_PORT, 3000);
	}

	public static void setPort(Configuration conf, int port) {
		conf.setInt(AEROSPIKE_PORT, port);
	}

	public static String getSetName(Configuration conf) {
		return conf.get(AEROSPIKE_SET);
	}

	public static void setSetName(Configuration conf, String setName) {
		conf.set(AEROSPIKE_SET, setName);
	}

	public static String getUser(Configuration conf) {
		return conf.get(AEROSPIKE_USER);
	}

	public static void setUser(Configuration conf, String user) {
		conf.set(AEROSPIKE_USER, user);
	}

	public static String getPassword(Configuration conf) {
		return conf.get(AEROSPIKE_PASSWORD);
	}

	public static void setPassword(Configuration conf, String password) {
		conf.set(AEROSPIKE_PASSWORD, password);
	}

}
