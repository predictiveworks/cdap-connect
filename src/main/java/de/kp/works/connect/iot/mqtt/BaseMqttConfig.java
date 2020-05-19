package de.kp.works.connect.iot.mqtt;
/*
 * Copyright (c) 2020 Dr. Krusche & Partner PartG. All rights reserved.
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

import java.util.Map;
import java.util.stream.Stream;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;

import de.kp.works.connect.SslConfig;
import de.kp.works.stream.ssl.*;

public class BaseMqttConfig extends SslConfig {

	protected static final long serialVersionUID = 5991732116957904344L;

	protected static final String USER_DESC = "The MQTT user name.";

	protected static final String PASSWORD_DESC = "The MQTT user password";

	protected static final String AUTH_METHOD_DESC = "The MQTT authentication method. Supported values "
			+ "are 'basic', 'ssl' and 'x509'. Default is 'basic'.";

	protected static final String MQTT_QOS_DESC = "The MQTT quality of service specification. Default is 'at_most_once";
	
	@Description(MQTT_QOS_DESC)
	@Macro
	public String mqttQoS;
	
	@Description(AUTH_METHOD_DESC)
	@Macro
	public String mqttAuth;

	@Description(USER_DESC)
	@Macro
	public String mqttUser;

	@Description(PASSWORD_DESC)
	@Macro
	public String mqttPass;

	public BaseMqttConfig() {
		super();

		mqttAuth = "basic";

	}

	public void validate() {
		super.validate();

		String className = this.getClass().getName();

		/* Validate authentication */

		if (Strings.isNullOrEmpty(mqttUser)) {
			throw new IllegalArgumentException(String.format("[%s] The MQTT user name must not be empty.", className));
		}

		if (Strings.isNullOrEmpty(mqttPass)) {
			throw new IllegalArgumentException(String.format("[%s] The MQTT password must not be empty.", className));
		}

		MqttAuth auth = getMqttAuth();
		switch (auth) {
		case BASIC: {
			break;
		}
		case SSL: {

			/*
			 * Support for SSL client-server authentication, based on existing key & trust
			 * store files; this information is used to build key & trust managers from
			 * existing key & trust stores.
			 */

			if (Strings.isNullOrEmpty(sslKeyStorePath)) {
				throw new IllegalArgumentException(
						String.format("[%s] The path to the keystore file must not be empty.", className));
			}

			if (Strings.isNullOrEmpty(sslKeyStorePass)) {
				throw new IllegalArgumentException(
						String.format("[%s] The password of the keystore must not be empty.", className));
			}

			if (sslVerify.equals("true")) {

				if (Strings.isNullOrEmpty(sslTrustStorePath)) {
					throw new IllegalArgumentException(
							String.format("[%s] The path to the truststore file must not be empty.", className));
				}

				if (Strings.isNullOrEmpty(sslTrustStorePass)) {
					throw new IllegalArgumentException(
							String.format("[%s] The password of the truststore must not be empty.", className));
				}

			}

			break;
		}
		case X509: {

			/*
			 * Support for SSL client-server authentication, based on a set of files that
			 * contain certificates and private key; this information is used to create
			 * respective key and truststores for authentication
			 */

			if (Strings.isNullOrEmpty(sslCaCertFile)) {
				throw new IllegalArgumentException(
						String.format("[%s] The path to the CA certificate file must not be empty.", className));
			}

			if (Strings.isNullOrEmpty(sslCertFile)) {
				throw new IllegalArgumentException(
						String.format("[%s] The path to the client certificate file must not be empty.", className));
			}

			if (Strings.isNullOrEmpty(sslPrivateKeyFile)) {
				throw new IllegalArgumentException(
						String.format("[%s] The path to the private key file must not be empty.", className));
			}

			if (Strings.isNullOrEmpty(sslPrivateKeyPass)) {
				throw new IllegalArgumentException(
						String.format("[%s] The password for the private key must not be empty.", className));
			}

			break;
		}
		}

	}

	public MqttAuth getMqttAuth() {

		Class<MqttAuth> enumClazz = MqttAuth.class;

		return Stream.of(enumClazz.getEnumConstants()).filter(keyType -> keyType.getValue().equalsIgnoreCase(mqttAuth))
				.findAny().orElseThrow(() -> new IllegalArgumentException(
						String.format("Unsupported value for authentication method: '%s'", mqttAuth)));

	}

	public MqttQoS getMqttQoS() {

		Class<MqttQoS> enumClazz = MqttQoS.class;

		return Stream.of(enumClazz.getEnumConstants()).filter(keyType -> keyType.getValue().equalsIgnoreCase(mqttQoS))
				.findAny().orElseThrow(() -> new IllegalArgumentException(
						String.format("Unsupported value for quality of service: '%s'", mqttQoS)));

	}

	public SSLOptions getMqttSsl(Map<String,String> mqttSecure) {

		/*
		 * This methods supports retrieval of secure information, e.g. passwords,
		 * certificates, private keys etc. from the CDAP internal secure store
		 */
		
		// TODO retrieve parameters from mqttSecure
		
		SSLOptions sslOptions = null;
		MqttAuth auth = getMqttAuth();

		switch (auth) {
		case BASIC: {
			break;
		}
		case SSL: {
			sslOptions = SSLOptionsUtil.buildStoreOptions(sslKeyStorePath, sslKeyStoreType, sslKeyStorePass,
					sslKeyStoreAlgo, sslTrustStorePath, sslTrustStoreType, sslTrustStorePass, sslTrustStoreAlgo);

			break;
		}
		case X509: {
			sslOptions = SSLOptionsUtil.buildCertFileOptions(sslCaCertFile, sslCertFile, sslPrivateKeyFile,
					sslPrivateKeyPass);

			break;
		}
		}

		return sslOptions;
	}
}
