package de.kp.works.connect.things.stream;
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
import de.kp.works.stream.things.ThingsNames;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.stream.Stream;

public class ThingsConfig extends SslConfig {

    @Description("The address of the MQTT broker to connect to, including protocol and port.")
    @Macro
    public String mqttBroker;

    @Description("The comma-separated list of MQTT topics to listen to.")
    @Macro
    public String mqttTopics;

    @Description("The MQTT user name.")
    @Macro
    @Nullable
    public String mqttUser;

    @Description("The MQTT user password")
    @Macro
    @Nullable
    public String mqttPass;

    @Description("The MQTT client identifier")
    @Macro
    @Nullable
    public String mqttClientId;

    @Description("The MQTT quality of service specification. Default is 'at_most_once")
    @Macro
    @Nullable
    public String mqttQoS;

    @Description("The version of the MQTT protocol. Default is 'mqtt_v31")
    @Macro
    @Nullable
    public String mqttVersion;

    public void validate() {

        String className = this.getClass().getName();

        if (Strings.isNullOrEmpty(mqttBroker)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The MQTT broker address must not be empty.", className));
        }

        if (Strings.isNullOrEmpty(mqttTopics)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The MQTT topics must not be empty.", className));
        }

    }

    public Properties toProperties() {

        Properties props = new Properties();

        props.setProperty(ThingsNames.BROKER_URL(), mqttBroker);
        props.setProperty(ThingsNames.TOPICS(), mqttTopics);

        if (!Strings.isNullOrEmpty(mqttUser)) {
            props.setProperty(ThingsNames.USERNAME(), mqttUser);
        }

        if (!Strings.isNullOrEmpty(mqttPass)) {
            props.setProperty(ThingsNames.PASSWORD(), mqttPass);
        }

        if (!Strings.isNullOrEmpty(mqttClientId)) {
            props.setProperty(ThingsNames.CLIENT_ID(), mqttClientId);
        }

        if (!Strings.isNullOrEmpty(mqttQoS)) {

            MqttQoS qos = getMqttQoS();

            int value = 1;
            if (qos.getValue().equals("at_most_once"))
                value = 0;

            if (qos.getValue().equals("exactly_once"))
                value = 2;

            props.setProperty(ThingsNames.QOS(), Integer.toString(value));

        }

        if (!Strings.isNullOrEmpty(mqttVersion)) {

            MqttVersion version = getMqttVersion();

            String value = "3.1";
            if (version.getValue().equals("mqtt_v311"))
                value = "3.1.1";

            props.setProperty(ThingsNames.VERSION(), value);

        }

        /* SSL SUPPORT */

        if (!Strings.isNullOrEmpty(sslCipherSuites)) {
            props.setProperty(ThingsNames.SSL_CIPHER_SUITES(), sslCipherSuites);
        }

        if (!Strings.isNullOrEmpty(sslKeyStorePath)) {
            props.setProperty(ThingsNames.SSL_KEYSTORE_FILE(), sslKeyStorePath);
        }

        if (!Strings.isNullOrEmpty(sslKeyStoreType)) {
            props.setProperty(ThingsNames.SSL_KEYSTORE_TYPE(), sslKeyStoreType);
        }

        if (!Strings.isNullOrEmpty(sslKeyStoreAlgo)) {
            props.setProperty(ThingsNames.SSL_KEYSTORE_ALGO(), sslKeyStoreAlgo);
        }

        if (!Strings.isNullOrEmpty(sslKeyStorePass)) {
            props.setProperty(ThingsNames.SSL_KEYSTORE_PASS(), sslKeyStorePass);
        }

        if (!Strings.isNullOrEmpty(sslTrustStorePath)) {
            props.setProperty(ThingsNames.SSL_TRUSTSTORE_FILE(), sslTrustStorePath);
        }

        if (!Strings.isNullOrEmpty(sslTrustStoreType)) {
            props.setProperty(ThingsNames.SSL_TRUSTSTORE_TYPE(), sslTrustStoreType);
        }

        if (!Strings.isNullOrEmpty(sslTrustStoreAlgo)) {
            props.setProperty(ThingsNames.SSL_TRUSTSTORE_ALGO(), sslTrustStoreAlgo);
        }

        if (!Strings.isNullOrEmpty(sslTrustStorePass)) {
            props.setProperty(ThingsNames.SSL_TRUSTSTORE_PASS(), sslTrustStorePass);
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
                        String.format("Unsupported value for quality of service: '%s'", mqttVersion)));

    }
}
