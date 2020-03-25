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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;

public class AerospikeConnect {
	
	public static final String AEROSPIKE_HOST = "aerospike.host";
	public static final String AEROSPIKE_PORT = "aerospike.port";
	
	public static final String AEROSPIKE_NAMESPACE = "aerospike.namespace";
	public static final String AEROSPIKE_SET = "aerospike.set";

	public static final String AEROSPIKE_EXPIRATION = "aerospike.expiration";
	
	public static final String AEROSPIKE_TIMEOUT = "aerospike.timeout";
	public static final String AEROSPIKE_WRITE = "aerospike.write";

	public static final String AEROSPIKE_USER = "aerospike.user";
	public static final String AEROSPIKE_PASSWORD = "aerospike.password";
	
	private Map<String, AerospikeClient> cache = new HashMap<>();
	
	public AerospikeClient getClient(Properties config) {
		
		synchronized(AerospikeConnect.class) {
			
			String host = config.getProperty(AEROSPIKE_HOST);
			String port = config.getProperty(AEROSPIKE_PORT);
			
			String key = String.format("%s:%s", host, port);
			if (cache.containsKey(key)) return cache.get(key);
			
			AerospikeClient client = createClient(config);
			cache.put(key, client);
			
			return client;
			
		}

	}
	
	private AerospikeClient createClient(Properties config) {
		
		String host = config.getProperty(AEROSPIKE_HOST);
		Integer port = Integer.valueOf(config.getProperty(AEROSPIKE_PORT));
		
		Integer timeout = Integer.valueOf(config.getProperty(AEROSPIKE_TIMEOUT));
		
		/* Define Client Policy */
		ClientPolicy policy = new ClientPolicy();
		policy.timeout = timeout;
		policy.failIfNotConnected = true;
		
		String user = config.getProperty(AEROSPIKE_USER);
		policy.user = user;
		
		String password = config.getProperty(AEROSPIKE_PASSWORD);
		policy.password = password;
		
		
		/* Create Client */
		AerospikeClient client = new AerospikeClient(policy, host, port);
		client.writePolicyDefault.totalTimeout = timeout;
	    client.readPolicyDefault.totalTimeout = timeout;
	    
	    client.scanPolicyDefault.totalTimeout = timeout;
	    client.queryPolicyDefault.totalTimeout = timeout;
	    
	    return client;
	    
	}
}
