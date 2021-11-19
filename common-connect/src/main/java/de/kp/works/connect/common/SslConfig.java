package de.kp.works.connect.common;
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import javax.annotation.Nullable;

public class SslConfig extends BaseConfig {
	
	private static final long serialVersionUID = 6634557540579917171L;
	
	protected static final String CA_CERT_DESC = "The path to the file that contains the CA certificate.";
	
	protected static final String CERT_DESC = "The path to the file that contains the client certificate.";
	
	protected static final String PRIVATE_KEY_DESC = "The path to the file that contains the private key.";
	
	protected static final String PRIVATE_KEY_PASSWORD_DESC = "The password to read the private key.";
	
	protected static final String CIPHER_SUITES_DESC = "A comma-separated list of cipher suites which are allowed for "
			+ "a secure connection. Samples are TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_RSA_WITH_AES_128_GCM_SHA256 and others.";

	protected static final String KEYSTORE_PATH_DESC = "A path to a file which contains the client SSL keystore.";

	protected static final String KEYSTORE_TYPE_DESC = "The format of the client SSL keystore. Supported values are 'JKS', "
			+ "'JCEKS' and 'PKCS12'. Default is 'JKS'.";

	protected static final String KEYSTORE_PASS_DESC = "The password of the client SSL keystore.";

	protected static final String KEYSTORE_ALGO_DESC = "The algorithm used for the client SSL keystore. Default is 'SunX509'.";
	
	protected static final String TRUSTSTORE_PATH_DESC = "A path to a file which contains the client SSL truststore.";

	protected static final String TRUSTSTORE_TYPE_DESC = "The format of the client SSL truststore. Supported values are 'JKS', "
			+ "'JCEKS' and 'PKCS12'. Default is 'JKS'.";

	protected static final String TRUSTSTORE_PASS_DESC = "The password of the client SSL truststore.";

	protected static final String TRUSTSTORE_ALGO_DESC = "The algorithm used for the client SSL truststore. Default is 'SunX509'.";
	
	protected static final String SSL_VERIFY_DESC = "An indicator to determine whether certificates have to be verified. "
			+ "Supported values are 'true' and 'false'. If 'false', untrusted trust "
			+ "certificates (e.g. self signed), will not lead to an error.";
	
	/*
	 * Support for SSL client-server authentication, based on a set of files
	 * that contain certificates and private key; this information is used to
	 * create respective key and truststores for authentication
	 */
	
	@Description(CA_CERT_DESC)
	@Macro
	@Nullable
	public String sslCaCertFile;

	@Description(CERT_DESC)
	@Macro
	@Nullable
	public String sslCertFile;

	@Description(PRIVATE_KEY_DESC)
	@Macro
	@Nullable
	public String sslPrivateKeyFile;

	@Description(PRIVATE_KEY_PASSWORD_DESC)
	@Macro
	@Nullable
	public String sslPrivateKeyPass;
	
	/*
	* Support for SSL client-server authentication, based on existing key & trust
	* store files; this information is used to build key & trust managers from
	* existing key & trust stores.
	*/

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

	@Description(KEYSTORE_PASS_DESC)
	@Macro
	@Nullable
	public String sslKeyStorePass;

	@Description(KEYSTORE_ALGO_DESC)
	@Macro
	@Nullable
	public String sslKeyStoreAlgo;

	@Description(TRUSTSTORE_PATH_DESC)
	@Macro
	@Nullable
	public String sslTrustStorePath;

	@Description(TRUSTSTORE_TYPE_DESC)
	@Macro
	@Nullable
	public String sslTrustStoreType;

	@Description(TRUSTSTORE_PASS_DESC)
	@Macro
	@Nullable
	public String sslTrustStorePass;

	@Description(TRUSTSTORE_ALGO_DESC)
	@Macro
	@Nullable
	public String sslTrustStoreAlgo;
	
	@Description(SSL_VERIFY_DESC)
	@Macro
	@Nullable
	public String sslVerify;
	
	public SslConfig() {
		super();
		
		sslVerify = "true";
		
		sslKeyStoreType = "JKS";
		sslKeyStoreAlgo = "SunX509";
		
		sslTrustStoreType = "JKS";
		sslTrustStoreAlgo = "SunX509";
		
	}
	
	public void validate() {
		super.validate();
		
	}
	
}
