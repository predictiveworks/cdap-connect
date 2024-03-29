package de.kp.works.connect.opencti;
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
import de.kp.works.stream.opencti.CTINames;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import javax.annotation.Nullable;
import java.util.Properties;

public class CTIConfig extends BaseConfig {

	protected static final String CIPHER_SUITES_DESC = "A comma-separated list of cipher suites which are allowed for "
			+ "a secure connection. Samples are TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_RSA_WITH_AES_128_GCM_SHA256 and others.";

	protected static final String KEYSTORE_ALGO_DESC = "The algorithm used for the client SSL keystore.";

	protected static final String KEYSTORE_PASS_DESC = "The password of the client SSL keystore.";

	protected static final String KEYSTORE_PATH_DESC = "A path to a file which contains the client SSL keystore.";

	protected static final String KEYSTORE_TYPE_DESC = "The format of the client SSL keystore. Supported values are 'JKS', "
			+ "'JCEKS' and 'PKCS12'. Default is 'JKS'.";

	protected static final String TRUSTSTORE_PATH_DESC = "A path to a file which contains the client SSL truststore.";

	protected static final String TRUSTSTORE_TYPE_DESC = "The format of the client SSL truststore. Supported values are 'JKS', "
			+ "'JCEKS' and 'PKCS12'. Default is 'JKS'.";

	protected static final String TRUSTSTORE_ALGO_DESC = "The algorithm used for the client SSL truststore.";

	protected static final String TRUSTSTORE_PASS_DESC = "The password of the client SSL truststore.";

	@Description("The URL of the OpenCTI server.")
	@Macro
	public String serverUrl;

	@Description("The access token to authenticate the OpenCTI user.")
	@Macro
	@Nullable
	public String authToken;

	@Description("The number of threads used to process OpenCTI. Default value is '1'.")
	@Macro
	@Nullable
	public String numThreads;
	/*
	 * TLS SECURITY
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

	public void validate() {
		super.validate();

		String className = this.getClass().getName();

		if (Strings.isNullOrEmpty(serverUrl)) {
			throw new IllegalArgumentException(
					String.format("[%s] The URL of the OpenCTI server must not be empty.", className));
		}

		if (Strings.isNullOrEmpty(sslKeyStorePath)) {
			throw new IllegalArgumentException(
					String.format("[%s] The path to the Keystore must not be empty.", className));
		}

	}

	public Properties toProperties() {

		Properties props = new Properties();
		props.setProperty(CTINames.SERVER_URL(), serverUrl);

		if (!Strings.isNullOrEmpty(authToken)) {
			props.setProperty(CTINames.AUTH_TOKEN(), authToken);
		}

		if (!Strings.isNullOrEmpty(numThreads)) {
			props.setProperty(CTINames.NUM_THREADS(), numThreads);
		}

		/* SSL SUPPORT */

		if (!Strings.isNullOrEmpty(sslCipherSuites)) {
			props.setProperty(CTINames.SSL_CIPHER_SUITES(), sslCipherSuites);
		}

		props.setProperty(CTINames.SSL_KEYSTORE_FILE(), sslKeyStorePath);

		if (!Strings.isNullOrEmpty(sslKeyStoreType)) {
			props.setProperty(CTINames.SSL_KEYSTORE_TYPE(), sslKeyStoreType);
		}

		if (!Strings.isNullOrEmpty(sslKeyStoreAlgo)) {
			props.setProperty(CTINames.SSL_KEYSTORE_ALGO(), sslKeyStoreAlgo);
		}

		if (!Strings.isNullOrEmpty(sslKeyStorePass)) {
			props.setProperty(CTINames.SSL_KEYSTORE_PASS(), sslKeyStorePass);
		}

		if (!Strings.isNullOrEmpty(sslTrustStorePath)) {
			props.setProperty(CTINames.SSL_TRUSTSTORE_FILE(), sslTrustStorePath);
		}

		if (!Strings.isNullOrEmpty(sslTrustStoreType)) {
			props.setProperty(CTINames.SSL_TRUSTSTORE_TYPE(), sslTrustStoreType);
		}

		if (!Strings.isNullOrEmpty(sslTrustStoreAlgo)) {
			props.setProperty(CTINames.SSL_TRUSTSTORE_ALGO(), sslTrustStoreAlgo);
		}

		if (!Strings.isNullOrEmpty(sslTrustStorePass)) {
			props.setProperty(CTINames.SSL_TRUSTSTORE_PASS(), sslTrustStorePass);
		}

		return props;
	}
}
