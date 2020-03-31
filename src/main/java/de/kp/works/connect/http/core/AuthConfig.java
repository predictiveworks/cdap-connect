package de.kp.works.connect.http.core;
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;

import de.kp.works.connect.ConfigUtil;
/*
 * AuthConfig --> ConnConfig --> BaseConfig
 */
public class AuthConfig extends ConnConfig {

	private static final long serialVersionUID = -814812639191567222L;
	
	public static final String PROPERTY_USERNAME = "username";
	public static final String PROPERTY_PASSWORD = "password";
	
	public static final String PROPERTY_PROXY_USERNAME = "proxyUsername";
	public static final String PROPERTY_PROXY_PASSWORD = "proxyPassword";

	public static final String PROPERTY_OAUTH2_ENABLED = "oauth2Enabled";
	
	public static final String PROPERTY_AUTH_URL = "authUrl";
	public static final String PROPERTY_TOKEN_URL = "tokenUrl";
	public static final String PROPERTY_CLIENT_ID = "clientId";
	public static final String PROPERTY_CLIENT_SECRET = "clientSecret";
	
	public static final String PROPERTY_SCOPES = "scopes";
	public static final String PROPERTY_REFRESH_TOKEN = "refreshToken";
	
	public static final String PROPERTY_VERIFY_HTTPS = "verifyHttps";
	
	public static final String PROPERTY_KEYSTORE_FILE = "keystoreFile";
	public static final String PROPERTY_KEYSTORE_TYPE = "keystoreType";
	public static final String PROPERTY_KEYSTORE_PASSWORD = "keystorePassword";
	public static final String PROPERTY_KEYSTORE_KEY_ALGORITHM = "keystoreKeyAlgorithm";

	public static final String PROPERTY_TRUSTSTORE_FILE = "trustStoreFile";
	public static final String PROPERTY_TRUSTSTORE_TYPE = "trustStoreType";
	public static final String PROPERTY_TRUSTSTORE_PASSWORD = "trustStorePassword";
	public static final String PROPERTY_TRUSTSTORE_KEY_ALGORITHM = "trustStoreKeyAlgorithm";

	public static final String PROPERTY_CIPHER_SUITES = "cipherSuites";
	
	/** BASIC AUTHENTICATION 
	 * 
	 * The Http Client performs basic authentication for use cases where 
	 * user name and password are provided. Then no other authentication
	 * method is used
	 */
	
	@Nullable
	@Name(PROPERTY_USERNAME)
	@Description("Username for basic authentication.")
	@Macro
	public String username;

	@Nullable
	@Name(PROPERTY_PASSWORD)
	@Description("Password for basic authentication.")
	@Macro
	public String password;

	/** PROXY AUTHENTICATION **/
	
	@Nullable
	@Name(PROPERTY_PROXY_USERNAME)
	@Description("Proxy username.")
	@Macro
	public String proxyUsername;

	@Nullable
	@Name(PROPERTY_PROXY_PASSWORD)
	@Description("Proxy password.")
	@Macro
	public String proxyPassword;

	/** OAUTH **/
	
	@Name(PROPERTY_OAUTH2_ENABLED)
	@Description("An indicator to determine whether OAuth2 authentication has to be performed. "
			+ "Supported values are 'true' and 'false'.")
	public String oauth2Enabled;

	@Nullable
	@Name(PROPERTY_AUTH_URL)
	@Description("Endpoint for the authorization server used to retrieve the authorization code.")
	@Macro
	public String authUrl;

	@Nullable
	@Name(PROPERTY_TOKEN_URL)
	@Description("Endpoint for the resource server, which exchanges the authorization code for an access token.")
	@Macro
	public String tokenUrl;

	@Nullable
	@Name(PROPERTY_SCOPES)
	@Description("Scope of the access request, which might have multiple space-separated values.")
	@Macro
	public String scopes;

	@Nullable
	@Name(PROPERTY_REFRESH_TOKEN)
	@Description("Token used to receive accessToken, which is end product of OAuth2.")
	@Macro
	public String refreshToken;

	@Nullable
	@Name(PROPERTY_CLIENT_ID)
	@Description("Client identifier obtained during the application registration process.")
	@Macro
	public String clientId;

	@Nullable
	@Name(PROPERTY_CLIENT_SECRET)
	@Description("Client secret obtained during the application registration process.")
	@Macro
	public String clientSecret;

	/** KEY & TRUST STORE **/

	@Name(PROPERTY_VERIFY_HTTPS)
	@Description("An indicator to determine whether certificates have to be verified. "
			+ "Supported values are 'true' and 'false'. If 'false', untrusted trust "
			+ "certificates (e.g. self signed), will not lead to an error. Do not disable "
			+ "this in production environment on a network you do not entirely trust.")
	@Macro
	public String verifyHttps;

	@Nullable
	@Name(PROPERTY_KEYSTORE_FILE)
	@Description("A path to a file which contains keystore.")
	@Macro
	public String keystoreFile;

	@Nullable
	@Name(PROPERTY_KEYSTORE_TYPE)
	@Description("Format of a keystore.")
	@Macro
	public String keystoreType;

	@Nullable
	@Name(PROPERTY_KEYSTORE_PASSWORD)
	@Description("Password for a keystore. If a keystore is not password protected leave it empty.")
	@Macro
	public String keystorePassword;

	@Nullable
	@Name(PROPERTY_KEYSTORE_KEY_ALGORITHM)
	@Description("An algorithm used for keystore.")
	@Macro
	public String keystoreKeyAlgorithm;

	@Nullable
	@Name(PROPERTY_TRUSTSTORE_FILE)
	@Description("A path to a file which contains truststore.")
	@Macro
	public String trustStoreFile;

	@Nullable
	@Name(PROPERTY_TRUSTSTORE_TYPE)
	@Description("Format of a truststore.")
	@Macro
	public String trustStoreType;

	@Nullable
	@Name(PROPERTY_TRUSTSTORE_PASSWORD)
	@Description("Password for a truststore. If a truststore is not password protected leave it empty.")
	@Macro
	public String trustStorePassword;

	@Nullable
	@Name(PROPERTY_TRUSTSTORE_KEY_ALGORITHM)
	@Description("An algorithm used for truststore.")
	@Macro
	public String trustStoreKeyAlgorithm;

	@Nullable
	@Name(PROPERTY_CIPHER_SUITES)
	@Description("Cipher suites which are allowed for connection. "
			+ "Colons, commas or spaces are also acceptable separators.")
	@Macro
	protected String cipherSuites;
	
	public AuthConfig() {
		super();
	}

	@Nullable
	public String getUsername() {
		return username;
	}

	@Nullable
	public String getPassword() {
		return password;
	}

	@Nullable
	public String getProxyUsername() {
		return proxyUsername;
	}

	@Nullable
	public String getProxyPassword() {
		return proxyPassword;
	}

	public Boolean getOauth2Enabled() {
		return Boolean.parseBoolean(oauth2Enabled);
	}

	@Nullable
	public String getAuthUrl() {
		return authUrl;
	}

	@Nullable
	public String getTokenUrl() {
		return tokenUrl;
	}

	@Nullable
	public String getScopes() {
		return scopes;
	}

	@Nullable
	public String getRefreshToken() {
		return refreshToken;
	}

	@Nullable
	public String getClientId() {
		return clientId;
	}

	@Nullable
	public String getClientSecret() {
		return clientSecret;
	}

	public Boolean getVerifyHttps() {
		return Boolean.parseBoolean(verifyHttps);
	}

	@Nullable
	public String getKeystoreFile() {
		return keystoreFile;
	}

	@Nullable
	public KeyStoreType getKeystoreType() {
		return ConfigUtil.getEnumValueByString(KeyStoreType.class, keystoreType, PROPERTY_KEYSTORE_TYPE);
	}

	@Nullable
	public String getKeystorePassword() {
		return keystorePassword;
	}

	@Nullable
	public String getKeystoreKeyAlgorithm() {
		return keystoreKeyAlgorithm;
	}

	@Nullable
	public String getTrustStoreFile() {
		return trustStoreFile;
	}

	@Nullable
	public KeyStoreType getTrustStoreType() {
		return ConfigUtil.getEnumValueByString(KeyStoreType.class, trustStoreType, PROPERTY_TRUSTSTORE_TYPE);
	}

	@Nullable
	public String getTrustStorePassword() {
		return trustStorePassword;
	}

	@Nullable
	public String getTrustStoreKeyAlgorithm() {
		return trustStoreKeyAlgorithm;
	}

	@Nullable
	public String getCipherSuites() {
		return cipherSuites;
	}
	
	public void validate() {
		super.validate();
		
		/** OAUTH2 **/

		if (!containsMacro(PROPERTY_OAUTH2_ENABLED) && this.getOauth2Enabled()) {
			
			String reasonOauth2 = "OAuth2 is enabled";
			ConfigUtil.assertIsSet(getAuthUrl(), PROPERTY_AUTH_URL, reasonOauth2);
			ConfigUtil.assertIsSet(getTokenUrl(), PROPERTY_TOKEN_URL, reasonOauth2);
			ConfigUtil.assertIsSet(getClientId(), PROPERTY_CLIENT_ID, reasonOauth2);
			ConfigUtil.assertIsSet(getClientSecret(), PROPERTY_CLIENT_SECRET, reasonOauth2);
			ConfigUtil.assertIsSet(getRefreshToken(), PROPERTY_REFRESH_TOKEN, reasonOauth2);
			
		}
		
		/** HTTPS */
		
		if (!containsMacro(PROPERTY_VERIFY_HTTPS) && !getVerifyHttps()) {
			ConfigUtil.assertIsNotSet(getTrustStoreFile(), PROPERTY_TRUSTSTORE_FILE,
					String.format("Trustore settings are ignored due to disabled %s", PROPERTY_VERIFY_HTTPS));
		}

	}
}
