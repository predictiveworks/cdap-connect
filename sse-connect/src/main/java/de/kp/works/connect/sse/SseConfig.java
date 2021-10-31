package de.kp.works.connect.sse;
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
import de.kp.works.stream.sse.SseNames;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import javax.annotation.Nullable;
import java.util.Properties;

public class SseConfig extends SslConfig {

    @Description("The URL of the SSE server.")
    @Macro
    public String serverUrl;

    @Description("The access token to authenticate the SSE user.")
    @Macro
    @Nullable
    public String authToken;

    @Description("The number of threads used to process SSE. Default value is '1'.")
    @Macro
    @Nullable
    public String numThreads;

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
        props.setProperty(SseNames.SERVER_URL(), serverUrl);

        if (!Strings.isNullOrEmpty(authToken)) {
            props.setProperty(SseNames.AUTH_TOKEN(), authToken);
        }

        if (!Strings.isNullOrEmpty(numThreads)) {
            props.setProperty(SseNames.NUM_THREADS(), numThreads);
        }

        /* SSL SUPPORT */

        if (!Strings.isNullOrEmpty(sslCipherSuites)) {
            props.setProperty(SseNames.SSL_CIPHER_SUITES(), sslCipherSuites);
        }

        props.setProperty(SseNames.SSL_KEYSTORE_FILE(), sslKeyStorePath);

        if (!Strings.isNullOrEmpty(sslKeyStoreType)) {
            props.setProperty(SseNames.SSL_KEYSTORE_TYPE(), sslKeyStoreType);
        }

        if (!Strings.isNullOrEmpty(sslKeyStoreAlgo)) {
            props.setProperty(SseNames.SSL_KEYSTORE_ALGO(), sslKeyStoreAlgo);
        }

        if (!Strings.isNullOrEmpty(sslKeyStorePass)) {
            props.setProperty(SseNames.SSL_KEYSTORE_PASS(), sslKeyStorePass);
        }

        if (!Strings.isNullOrEmpty(sslTrustStorePath)) {
            props.setProperty(SseNames.SSL_TRUSTSTORE_FILE(), sslTrustStorePath);
        }

        if (!Strings.isNullOrEmpty(sslTrustStoreType)) {
            props.setProperty(SseNames.SSL_TRUSTSTORE_TYPE(), sslTrustStoreType);
        }

        if (!Strings.isNullOrEmpty(sslTrustStoreAlgo)) {
            props.setProperty(SseNames.SSL_TRUSTSTORE_ALGO(), sslTrustStoreAlgo);
        }

        if (!Strings.isNullOrEmpty(sslTrustStorePass)) {
            props.setProperty(SseNames.SSL_TRUSTSTORE_PASS(), sslTrustStorePass);
        }

        return props;

    }
}
