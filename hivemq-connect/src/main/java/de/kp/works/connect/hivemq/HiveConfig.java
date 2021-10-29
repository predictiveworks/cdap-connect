package de.kp.works.connect.hivemq;
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
import de.kp.works.connect.common.SslConfig;
import de.kp.works.stream.mqtt.MqttNames;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.stream.Stream;

public class HiveConfig extends SslConfig {

	private static final long serialVersionUID = 3127652226872012920L;

	@Description("The host of the HiveMQ broker.")
	@Macro
	public String mqttHost;

	@Description("The port of the HiveMQ broker.")
	@Macro
	public Integer mqttPort;

	@Description("The MQTT user name.")
	@Macro
	@Nullable
	public String mqttUser;

	@Description("The MQTT user password")
	@Macro
	@Nullable
	public String mqttPass;

	@Description("The MQTT quality of service specification. Default is 'at_most_once")
	@Macro
	@Nullable
	public String mqttQoS;

	@Description("The version of the MQTT protocol. Default is 'mqtt_v31")
	@Macro
	@Nullable
	public String mqttVersion;

	public void validate() {
		super.validate();

		String className = this.getClass().getName();
		
		if (Strings.isNullOrEmpty(mqttHost)) {
			throw new IllegalArgumentException(
					String.format("[%s] The MQTT host must not be empty.", className));
		}

		if (mqttPort < 1) {
			throw new IllegalArgumentException(
					String.format("[%s] The MQTT port must be positive.", className));
		}

	}

	public Properties toProperties() {

		Properties props = new Properties();

		props.setProperty(MqttNames.HOST(), mqttHost);
		props.setProperty(MqttNames.PORT(), Integer.toString(mqttPort));

		if (!Strings.isNullOrEmpty(mqttUser)) {
			props.setProperty(MqttNames.USERNAME(), mqttUser);
		}

		if (!Strings.isNullOrEmpty(mqttPass)) {
			props.setProperty(MqttNames.PASSWORD(), mqttPass);
		}

		if (!Strings.isNullOrEmpty(mqttQoS)) {

			MqttQoS qos = getMqttQoS();

			int value = 1;
			if (qos.getValue().equals("at_most_once"))
				value = 0;

			if (qos.getValue().equals("exactly_once"))
				value = 2;

			props.setProperty(MqttNames.QOS(), Integer.toString(value));

		}

		if (!Strings.isNullOrEmpty(mqttVersion)) {

			MqttVersion version = getMqttVersion();

			String value = "5";
			if (version.getValue().equals("mqtt_v311"))
				value = "3.1.1";

			props.setProperty(MqttNames.VERSION(), value);

		}

		/* SSL SUPPORT */

		if (!Strings.isNullOrEmpty(sslCipherSuites)) {
			props.setProperty(MqttNames.SSL_CIPHER_SUITES(), sslCipherSuites);
		}

		if (!Strings.isNullOrEmpty(sslKeyStorePath)) {
			props.setProperty(MqttNames.SSL_KEYSTORE_FILE(), sslKeyStorePath);
		}

		if (!Strings.isNullOrEmpty(sslKeyStoreType)) {
			props.setProperty(MqttNames.SSL_KEYSTORE_TYPE(), sslKeyStoreType);
		}

		if (!Strings.isNullOrEmpty(sslKeyStoreAlgo)) {
			props.setProperty(MqttNames.SSL_KEYSTORE_ALGO(), sslKeyStoreAlgo);
		}

		if (!Strings.isNullOrEmpty(sslKeyStorePass)) {
			props.setProperty(MqttNames.SSL_KEYSTORE_PASS(), sslKeyStorePass);
		}

		if (!Strings.isNullOrEmpty(sslTrustStorePath)) {
			props.setProperty(MqttNames.SSL_TRUSTSTORE_FILE(), sslTrustStorePath);
		}

		if (!Strings.isNullOrEmpty(sslTrustStoreType)) {
			props.setProperty(MqttNames.SSL_TRUSTSTORE_TYPE(), sslTrustStoreType);
		}

		if (!Strings.isNullOrEmpty(sslTrustStoreAlgo)) {
			props.setProperty(MqttNames.SSL_TRUSTSTORE_ALGO(), sslTrustStoreAlgo);
		}

		if (!Strings.isNullOrEmpty(sslTrustStorePass)) {
			props.setProperty(MqttNames.SSL_TRUSTSTORE_PASS(), sslTrustStorePass);
		}

		return props;

	}

	private MqttQoS getMqttQoS() {

		Class<MqttQoS> enumClazz = MqttQoS.class;

		return Stream.of(enumClazz.getEnumConstants()).filter(keyType -> keyType.getValue().equalsIgnoreCase(mqttQoS))
				.findAny().orElseThrow(() -> new IllegalArgumentException(
						String.format("Unsupported value for quality of service: '%s'", mqttQoS)));

	}

	private MqttVersion getMqttVersion() {

		Class<MqttVersion> enumClazz = MqttVersion.class;

		return Stream.of(enumClazz.getEnumConstants()).filter(keyType -> keyType.getValue().equalsIgnoreCase(mqttVersion))
				.findAny().orElseThrow(() -> new IllegalArgumentException(
						String.format("Unsupported value for MQTT version: '%s'", mqttVersion)));

	}

}

