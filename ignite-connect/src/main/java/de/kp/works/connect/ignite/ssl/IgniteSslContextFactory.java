package de.kp.works.connect.ignite.ssl;
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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import javax.cache.configuration.Factory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import de.kp.works.connect.common.DisabledTrustManager;
import org.apache.ignite.IgniteException;
import com.google.common.base.Strings;

import de.kp.works.connect.ignite.IgniteUtil;
import org.apache.ignite.ssl.SSLContextWrapper;

/**
 * This is a re-implementation of Apache Ignite's SslContextFactory 
 * to enable PredictiveWorks. specific adjustments
 */
public class IgniteSslContextFactory implements Factory<SSLContext>{

	private static final long serialVersionUID = 7621139595929347183L;

	private final Properties props;
	private final static String SSL_PROTOCOL = "TLS";
	
    private final AtomicReference<SSLContext> sslCtx = new AtomicReference<>();
	
	public IgniteSslContextFactory(Properties props) {
		this.props = props;
	}
	
    private SSLContext createSslContext() throws SSLException {

   		SSLContext sslContext;
   		
		try {
			/*
			 * The SSL protocol is currently hard coded
			 * and set to TLS
			 */
			sslContext = SSLContext.getInstance(SSL_PROTOCOL);
			/*
			 * Extract cipher suites
			 */
			String sslCipherSuites = props.getProperty(IgniteUtil.IGNITE_SSL_CIPHER_SUITES);
			String[] cipherSuites = (sslCipherSuites == null) ? null : IgniteUtil.string2Array(sslCipherSuites);
	 			
            if (cipherSuites != null) {
            	
                SSLParameters sslParameters = new SSLParameters();
                sslParameters.setCipherSuites(cipherSuites);

                sslContext = new SSLContextWrapper(sslContext, sslParameters);
		
            }
            
			/* Build key managers */
			KeyManager[] keyManagers = getKeyManagers();

			/* Build trust managers */
			TrustManager[] trustManagers = getTrustManagers();

			
			
			/* Build SSL context */
			sslContext.init(keyManagers, trustManagers, null);
            return sslContext;			
			
		} catch (Exception e) {
			throw new SSLException(e);

		}

    }	

	private KeyManager[] getKeyManagers() throws SSLException {

		String keyStorePath = props.getProperty(IgniteUtil.IGNITE_SSL_KEYSTORE_PATH);
		String keyStoreType = props.getProperty(IgniteUtil.IGNITE_SSL_KEYSTORE_TYPE);

		String keyStorePass = props.getProperty(IgniteUtil.IGNITE_SSL_KEYSTORE_PASS);
		KeyStore keyStore = loadKeystore(keyStorePath, keyStoreType, keyStorePass);

		/*
		 * We have to manually fall back to default keystore. 
		 * SSLContext won't provide such a functionality.
		 */
		KeyManager[] keyManagers;
		try {

			String keyStoreAlgorithm = (props.containsKey(IgniteUtil.IGNITE_SSL_KEYSTORE_ALGO)) ? props.getProperty(IgniteUtil.IGNITE_SSL_KEYSTORE_ALGO) : null;
			if (Strings.isNullOrEmpty(keyStoreAlgorithm))
				keyStoreAlgorithm = KeyManagerFactory.getDefaultAlgorithm();

			KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(keyStoreAlgorithm);
			char[] passArray = keyStorePass.toCharArray();
			
			keyManagerFactory.init(keyStore, passArray);
			keyManagers = keyManagerFactory.getKeyManagers();
			
		} catch (Exception e) {
			throw new SSLException(e);
		}
		
		return keyManagers;
	}
	
	private TrustManager[] getTrustManagers() throws SSLException {

		/* Check whether we trust all certificates */
		String sslVerify = props.getProperty(IgniteUtil.IGNITE_SSL_VERIFY);
		if (sslVerify.equals("true"))
			return new TrustManager[] { new DisabledTrustManager() };

		String trustStorePath = props.getProperty(IgniteUtil.IGNITE_SSL_TRUSTSTORE_PATH);
		String trustStoreType = props.getProperty(IgniteUtil.IGNITE_SSL_TRUSTSTORE_TYPE);

		String trustStorePass = props.getProperty(IgniteUtil.IGNITE_SSL_TRUSTSTORE_PASS);
		KeyStore trustStore = loadKeystore(trustStorePath, trustStoreType, trustStorePass);

		TrustManager[] trustManagers;

		try {

			String trustStoreAlgorithm = (props.containsKey(IgniteUtil.IGNITE_SSL_TRUSTSTORE_ALGO)) ? props.getProperty(IgniteUtil.IGNITE_SSL_TRUSTSTORE_ALGO) : null;
			if (Strings.isNullOrEmpty(trustStoreAlgorithm))
				trustStoreAlgorithm = TrustManagerFactory.getDefaultAlgorithm();

			TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(trustStoreAlgorithm);
			trustManagerFactory.init(trustStore);

			trustManagers = trustManagerFactory.getTrustManagers();

		} catch (Exception e) {
			throw new SSLException(e);
		}

		return trustManagers;

	}

	private KeyStore loadKeystore(String keystorePath, String keystoreType, String keystorePassword)
			throws SSLException {

		KeyStore keystore;

		try {

			keystore = KeyStore.getInstance(keystoreType);
			char[] passArray = (keystorePassword == null) ? null : keystorePassword.toCharArray();

			InputStream is = Files.newInputStream(Paths.get(keystorePath));
			keystore.load(is, passArray);

			return keystore;

		} catch (Exception e) {
			throw new SSLException(e);
		}

	}
	
	@Override
	public SSLContext create() {
		
        SSLContext ctx = sslCtx.get();

        if (ctx == null) {
            try {
                ctx = createSslContext();

                if (!sslCtx.compareAndSet(null, ctx))
                    ctx = sslCtx.get();
            }
            catch (SSLException e) {
                throw new IgniteException(e);
            }
        }

        return ctx;
        
	}

}
