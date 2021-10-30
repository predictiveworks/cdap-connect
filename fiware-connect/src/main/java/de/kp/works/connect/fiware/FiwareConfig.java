package de.kp.works.connect.fiware;
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
import de.kp.works.stream.fiware.FiwareNames;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import javax.annotation.Nullable;
import java.util.Properties;

public class FiwareConfig extends BaseConfig {

    @Description("The host address of the Fiware notification endpoint.")
    @Macro
    public String serverHost;

    @Description("The port of the Fiware notification endpoint.")
    @Macro
    public String serverPort;

    @Description("The url of the Fiware broker.")
    @Macro
    public String brokerUrl;

    @Description("The Fiware Broker subscription(s).")
    @Macro
    public String brokerSubscriptions;

    @Description("The number of threads used to process Fiware notifications. Default value is '1'.")
    @Macro
    @Nullable
    public String numThreads;

    /*
     * SERVER SSL
     */
    @Description("A path to a file which contains the client SSL keystore of the Fiware server.")
    @Macro
    public String serverSslKeyStorePath;

    @Description("The format of the client SSL keystore of the Fiware server. Supported values are 'JKS', "
            + "'JCEKS' and 'PKCS12'. Default is 'JKS'.")
    @Macro
    @Nullable
    public String serverSslKeyStoreType;

    @Description("The algorithm used for the client SSL keystore of the Fiware server. Default is 'SunX509'.")
    @Macro
    @Nullable
    public String serverSslKeyStoreAlgo;

    @Description("The password of the client SSL keystore of the Fiware server.")
    @Macro
    @Nullable
    public String serverSslKeyStorePass;

    @Description("A path to a file which contains the client SSL truststore of the Fiware server.")
    @Macro
    @Nullable
    public String serverSslTrustStorePath;

    @Description("The format of the client SSL truststore of the Fiware server. Supported values are 'JKS', "
            + "'JCEKS' and 'PKCS12'. Default is 'JKS'.")
    @Macro
    @Nullable
    public String serverSslTrustStoreType;

    @Description("The algorithm used for the client SSL truststore of the Fiware server. Default is 'SunX509'.")
    @Macro
    @Nullable
    public String serverSslTrustStoreAlgo;

    @Description("The password of the client SSL truststore of the Fiware server.")
    @Macro
    @Nullable
    public String serverSslTrustStorePass;

    @Description("A comma-separated list of cipher suites which are allowed for "
            + "a secure Fiware server connection.")
    @Macro
    @Nullable
    public String serverSslCipherSuites;

    /*
     * BROKER SSL
     */
    @Description("A path to a file which contains the client SSL keystore for the Fiware Broker.")
    @Macro
    public String brokerSslKeyStorePath;

    @Description("The format of the client SSL keystore for the Fiware server. Supported values are 'JKS', "
            + "'JCEKS' and 'PKCS12'. Default is 'JKS'.")
    @Macro
    @Nullable
    public String brokerSslKeyStoreType;

    @Description("The algorithm used for the client SSL keystore of the Fiware broker. Default is 'SunX509'.")
    @Macro
    @Nullable
    public String brokerSslKeyStoreAlgo;

    @Description("The password of the client SSL keystore of the Fiware broker.")
    @Macro
    @Nullable
    public String brokerSslKeyStorePass;

    @Description("A path to a file which contains the client SSL truststore of the Fiware broker.")
    @Macro
    @Nullable
    public String brokerSslTrustStorePath;

    @Description("The format of the client SSL truststore of the Fiware broker. Supported values are 'JKS', "
            + "'JCEKS' and 'PKCS12'. Default is 'JKS'.")
    @Macro
    @Nullable
    public String brokerSslTrustStoreType;

    @Description("The algorithm used for the client SSL truststore of the Fiware broker. Default is 'SunX509'.")
    @Macro
    @Nullable
    public String brokerSslTrustStoreAlgo;

    @Description("The password of the client SSL truststore of the Fiware broker.")
    @Macro
    @Nullable
    public String brokerSslTrustStorePass;

    @Description("A comma-separated list of cipher suites which are allowed for "
            + "a secure Fiware Broker connection.")
    @Macro
    @Nullable
    public String brokerSslCipherSuites;

    public void validate() {
        super.validate();

        String className = this.getClass().getName();

        if (Strings.isNullOrEmpty(serverHost)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The server host must not be empty.", className));
        }

        if (Strings.isNullOrEmpty(serverPort)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The server port must not be empty.", className));
        }

        if (Strings.isNullOrEmpty(brokerUrl)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The url of the Fiware Context Broker must not be empty.", className));
        }

        if (Strings.isNullOrEmpty(brokerSubscriptions)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The Fiware Broker subscription(s) must not be empty.", className));
        }

        if (Strings.isNullOrEmpty(serverSslKeyStorePath)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The path to the Fiware server Keystore must not be empty.", className));
        }

        if (Strings.isNullOrEmpty(brokerSslKeyStorePath)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The path to the Fiware Context Broker Keystore must not be empty.", className));
        }

    }

    public Properties toProperties() {

        Properties props = new Properties();
        /*
         * Server HTTP binding
         */
        props.setProperty(FiwareNames.SERVER_HOST(), serverHost);
        props.setProperty(FiwareNames.SERVER_PORT(), serverPort);
        /*
         * Broker Url & Subscriptions
         */
        props.setProperty(FiwareNames.BROKER_URL(), brokerUrl);
        props.setProperty(FiwareNames.BROKER_SUBSCRIPTIONS(), brokerSubscriptions);

        if (!Strings.isNullOrEmpty(numThreads)) {
            props.setProperty(FiwareNames.NUM_THREADS(), numThreads);
        }
        /*
         * SSL configuration for Fiware server
         */

        if (!Strings.isNullOrEmpty(serverSslCipherSuites)) {
            props.setProperty(FiwareNames.SERVER_SSL_CIPHER_SUITES(), serverSslCipherSuites);
        }

        props.setProperty(FiwareNames.SERVER_SSL_KEYSTORE_FILE(), serverSslKeyStorePath);

        if (!Strings.isNullOrEmpty(serverSslKeyStoreType)) {
            props.setProperty(FiwareNames.SERVER_SSL_KEYSTORE_TYPE(), serverSslKeyStoreType);
        }

        if (!Strings.isNullOrEmpty(serverSslKeyStoreAlgo)) {
            props.setProperty(FiwareNames.SERVER_SSL_KEYSTORE_ALGO(), serverSslKeyStoreAlgo);
        }

        if (!Strings.isNullOrEmpty(serverSslKeyStorePass)) {
            props.setProperty(FiwareNames.SERVER_SSL_KEYSTORE_PASS(), serverSslKeyStorePass);
        }

        if (!Strings.isNullOrEmpty(serverSslTrustStorePath)) {
            props.setProperty(FiwareNames.SERVER_SSL_TRUSTSTORE_FILE(), serverSslTrustStorePath);
        }

        if (!Strings.isNullOrEmpty(serverSslTrustStoreType)) {
            props.setProperty(FiwareNames.SERVER_SSL_TRUSTSTORE_TYPE(), serverSslTrustStoreType);
        }

        if (!Strings.isNullOrEmpty(serverSslTrustStoreAlgo)) {
            props.setProperty(FiwareNames.SERVER_SSL_TRUSTSTORE_ALGO(), serverSslTrustStoreAlgo);
        }

        if (!Strings.isNullOrEmpty(serverSslTrustStorePass)) {
            props.setProperty(FiwareNames.SERVER_SSL_TRUSTSTORE_PASS(), serverSslTrustStorePass);
        }

        /*
         * SSL configuration for Fiware Context Broker
         */

        if (!Strings.isNullOrEmpty(brokerSslCipherSuites)) {
            props.setProperty(FiwareNames.BROKER_SSL_CIPHER_SUITES(), brokerSslCipherSuites);
        }

        props.setProperty(FiwareNames.BROKER_SSL_KEYSTORE_FILE(), brokerSslKeyStorePath);

        if (!Strings.isNullOrEmpty(brokerSslKeyStoreType)) {
            props.setProperty(FiwareNames.BROKER_SSL_KEYSTORE_TYPE(), brokerSslKeyStoreType);
        }

        if (!Strings.isNullOrEmpty(brokerSslKeyStoreAlgo)) {
            props.setProperty(FiwareNames.BROKER_SSL_KEYSTORE_ALGO(), brokerSslKeyStoreAlgo);
        }

        if (!Strings.isNullOrEmpty(brokerSslKeyStorePass)) {
            props.setProperty(FiwareNames.BROKER_SSL_KEYSTORE_PASS(), brokerSslKeyStorePass);
        }

        if (!Strings.isNullOrEmpty(brokerSslTrustStorePath)) {
            props.setProperty(FiwareNames.BROKER_SSL_TRUSTSTORE_FILE(), brokerSslTrustStorePath);
        }

        if (!Strings.isNullOrEmpty(brokerSslTrustStoreType)) {
            props.setProperty(FiwareNames.BROKER_SSL_TRUSTSTORE_TYPE(), brokerSslTrustStoreType);
        }

        if (!Strings.isNullOrEmpty(brokerSslTrustStoreAlgo)) {
            props.setProperty(FiwareNames.BROKER_SSL_TRUSTSTORE_ALGO(), brokerSslTrustStoreAlgo);
        }

        if (!Strings.isNullOrEmpty(brokerSslTrustStorePass)) {
            props.setProperty(FiwareNames.BROKER_SSL_TRUSTSTORE_PASS(), brokerSslTrustStorePass);
        }

        return props;
    }

}
