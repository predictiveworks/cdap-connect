package de.kp.works.connect;
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

import javax.annotation.Nullable;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;

public class SslConfig extends BaseConfig {
	
	private static final long serialVersionUID = 6634557540579917171L;

	@Description("An indicator to determine whether SSL has to be used to secure a remote "
			+ "connection. Supported values are 'true' and 'false'. Default is 'true'.")
	@Macro
	public String sslMode;

	@Description("An indicator to determine whether certificates have to be verified. "
			+ "Supported values are 'true' and 'false'. If 'false', untrusted trust "
			+ "certificates (e.g. self signed), will not lead to an error. Do not disable "
			+ "this in production environment on a network you do not entirely trust.")
	@Macro
	@Nullable
	public String sslVerify;

	/** KEY STORE **/

	@Description("A comma-separated list of cipher suites which are allowed for a secure connection. "
			+ "Samples are TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_RSA_WITH_AES_128_GCM_SHA256 and others.")
	@Macro
	@Nullable
	public String sslCipherSuites;
	
	@Description("A path to a file which contains the client SSL keystore.")
	@Macro
	@Nullable
	public String sslKeyStorePath;

	@Description("The format of the client SSL keystore. Suported values are 'JKS', 'JCEKS' and 'PKCS12'. Default is 'JKS'.")
	@Macro
	@Nullable
	public String sslKeyStoreType;

	@Description("The password of the client SSL keystore.")
	@Macro
	@Nullable
	public String sslKeyStorePass;

	@Description("The algorithm used for the client SSL keystore. Default is 'SunX509'.")
	@Macro
	@Nullable
	public String sslKeyStoreAlgo;
	
	/** TRUST STORE **/

	@Description("A path to a file which contains the client SSL truststore.")
	@Macro
	@Nullable
	public String sslTrustStorePath;

	@Description("The format of the client SSL truststore. Suported values are 'JKS', 'JCEKS' and 'PKCS12'. Default is 'JKS'.")
	@Macro
	@Nullable
	public String sslTrustStoreType;

	@Description("The password of the client SSL truststore.")
	@Macro
	@Nullable
	public String sslTrustStorePass;

	@Description("The algorithm used for the client SSL truststore. Default is 'SunX509'.")
	@Macro
	@Nullable
	public String sslTrustStoreAlgo;
	
	public SslConfig() {
		super();
		
		sslMode = "true";
		sslVerify = "true";
		
		sslKeyStoreType = "JKS";
		sslKeyStoreAlgo = "SunX509";
		
		sslTrustStoreType = "JKS";
		sslTrustStoreAlgo = "SunX509";
		
	}
	
	public void validate() {
		super.validate();
		
		if (Strings.isNullOrEmpty(sslMode)) {
			throw new IllegalArgumentException(
					String.format("[%s] The SSL indicator must not be empty.", this.getClass().getName()));
		}
		
	}
	
}
