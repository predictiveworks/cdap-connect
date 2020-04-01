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

import java.util.Properties;

import org.apache.directory.api.util.Strings;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

public class IgniteContext {
	/*
	 * Reference to Apache Ignite that is transferred to the key value
	 * store to enable cache operations
	 */
	private IgniteClient ignite = null;

	private static IgniteContext instance;

	private IgniteContext(Properties props) {

		if (props == null)
			throw new IllegalArgumentException("No configuration for Ignite client provided");
		
		ClientConfiguration config = new ClientConfiguration();
		/*
		 * Ignite binary client protocol enables user applications 
		 * to communicate with an existing Ignite cluster without 
		 * starting a full-fledged Ignite node. 
		 * 
		 * An application can connect to the cluster through a raw 
		 * TCP socket. Once the connection is established, the 
		 * application can communicate with the Ignite cluster and 
		 * perform cache operations using the established format.
		 */
		String host = props.getProperty("ignite.host");
		String port = props.getProperty("ignite.port");

		String address = String.format("%s:%s", host, port);
		config.setAddresses(address);
		
		/* BASIC AUTHENTICATION */
		
		String user = props.containsKey("ignite.user") ? props.getProperty("ignite.user") : null;
		String pass = props.containsKey("ignite.password") ? props.getProperty("ignite.password") : null;

		if (Strings.isNotEmpty(user) && Strings.isNotEmpty(pass)) {

			config.setUserName(user);
			config.setUserPassword(pass);

		}

		/* SSL SECURITY */
		
		setSecurity(config, props);
		ignite = Ignition.startClient(config);
		
	}

	private void setSecurity(ClientConfiguration config, Properties props) {

		if (props.containsKey("ignite.ssl")) {

			String igniteSsl = props.getProperty("ignite.ssl");
			if (igniteSsl.equals("true")) {
				/*

        			clientCfg
        			    .setSslMode(SslMode.REQUIRED)
        			    .setSslClientCertificateKeyStorePath("client.jks")
        			    .setSslClientCertificateKeyStoreType("JKS")
        			    .setSslClientCertificateKeyStorePassword("123456")
        			    .setSslTrustCertificateKeyStorePath("trust.jks")
        			    .setSslTrustCertificateKeyStoreType("JKS")
        			    .setSslTrustCertificateKeyStorePassword("123456")
        			    .setSslKeyAlgorithm("SunX509")
        			    .setSslTrustAll(false)
        			    .setSslProtocol(SslProtocol.TLS);
				 */


			}
		}

	}

	/*
	 * This method should be used after having Ignite
	 * properly configured and started
	 */
	public static IgniteContext getInstance() {
		return getInstance(null);
	}

	public static IgniteContext getInstance(Properties props) {
		if (instance == null) instance = new IgniteContext(props);
		return instance;
	}

	public IgniteClient getClient() {
		return ignite;
	}

}
