package de.kp.works.connect.iot.ditto;
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

import java.util.Properties;

import javax.annotation.Nullable;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import com.google.common.base.Strings;

import de.kp.works.connect.BaseConfig;
import de.kp.works.ditto.DittoUtils;

public class DittoConfig extends BaseConfig {

	private static final long serialVersionUID = 3823407192490514926L;

	/** ENDPOINT **/
	
	@Description("The endpoint of the Thing service endpoint.")
	@Macro
	public String endpoint;
 
	/** PROXY **/
	
	@Description("The proxy host of the Thing service endpoint.")
	@Macro
	@Nullable
	public String proxyHost;
	
	@Description("The proxy port of the Thing service endpoint.")
	@Macro
	@Nullable
	public String proxyPort;
	
	/** BASIC AUTHENTICATION **/
	
	@Description("The user name of the Thing service.")
	@Macro
	@Nullable
	public String user;
	
	@Description("The user password of the Thing service.")
	@Macro
	@Nullable
	public String password;
	
	/** OAUTH **/

	@Description("Client identifier obtained during the registration process.")
	@Macro
	@Nullable
	public String clientId;

	@Description("Client secret obtained during the registration process.")
	@Macro
	@Nullable
	public String clientSecret;

	@Description("Endpoint for the resource server, which exchanges the authorization code for an access token.")
	@Macro
	@Nullable
	public String tokenEndpoint;

	@Description("Scope of the access request, which might have multiple comma-separated values.")
	@Macro
	@Nullable
	public String scopes;

	/** TRUST STORE **/
	
	@Description("A path to a file which contains the truststore.")
	@Macro
	@Nullable
	public String trustLocation;

	@Description("Password for a truststore.")
	@Macro
	@Nullable
	public String trustPassword;
	
	/** THING & FEATURE **/

	@Description("The unique identifier of a certain thing. if provided, thing specific subscriptions are restricted to this thing.")
	@Macro
	@Nullable
	public String thingId;

	@Description("The unique identifier of a certain feature. if provided, feature specific subscriptions are restricted to this feature.")
	@Macro
	@Nullable
	public String featureId;
	
	
	/** CHANGE EVENTS **/
      
	@Description("An indicator to determine whether to listed to changes of all things. Supported values are 'true' and 'false'. Default is 'true'.")
	@Macro
	@Nullable
	public String thingChanges;
    
	@Description("An indicator to determine whether to listed to all feature set changes of all things. Supported values are 'true' and 'false'. Default is 'false'.")
	@Macro
	@Nullable
	public String featuresChanges;
    
	@Description("An indicator to determine whether to listed to all feature changes of all things. Supported values are 'true' and 'false'. Default is 'false'.")
	@Macro
	@Nullable
	public String featureChanges;
       
    /** LIVE MESSAGES **/
    
	@Description("An indicator to determine whether to listed to all message sent by of all things. Supported values are 'true' and 'false'. Default is 'false'.")
	@Macro
	@Nullable
	public String liveMessages;
	
	public Properties getThingConf() {
		
		Properties props = new Properties();

		/* ENDPOINT */

		props.setProperty(DittoUtils.DITTO_ENDPOINT(), endpoint);
		
		/* PROXY */
		
		props.setProperty(DittoUtils.DITTO_PROXY_HOST(), proxyHost);		
		props.setProperty(DittoUtils.DITTO_PROXY_PORT(), proxyPort);
		
		/* BASIC AUTHENTICATION */
		
		props.setProperty(DittoUtils.DITTO_USER(), user);		
		props.setProperty(DittoUtils.DITTO_PASS(), password);
		
		/* OAUTH */
		
		props.setProperty(DittoUtils.DITTO_OAUTH_CLIENT_ID(), clientId);		
		props.setProperty(DittoUtils.DITTO_OAUTH_CLIENT_SECRET(), clientSecret);
		props.setProperty(DittoUtils.DITTO_OAUTH_TOKEN_ENDPOINT(), tokenEndpoint);
		props.setProperty(DittoUtils.DITTO_OAUTH_SCOPES(), scopes);
		
		/* TRUST STORE */
		
		props.setProperty(DittoUtils.DITTO_TRUSTSTORE_LOCATION(), trustLocation);		
		props.setProperty(DittoUtils.DITTO_TRUSTSTORE_PASSWORD(), trustPassword);		
		
		/* CHANGE EVENTS */

		props.setProperty(DittoUtils.DITTO_THING_CHANGES(), thingChanges);
		props.setProperty(DittoUtils.DITTO_FEATURES_CHANGES(), featuresChanges);
		props.setProperty(DittoUtils.DITTO_FEATURE_CHANGES(), featureChanges);
		
		/* LIVE CHANGES */

		props.setProperty(DittoUtils.DITTO_LIVE_MESSAGES(), liveMessages);
		
		/* THING & FEATURE */
		
		if (!Strings.isNullOrEmpty(thingId))
			props.setProperty(DittoUtils.DITTO_THING_ID(), thingId);
		
		if (!Strings.isNullOrEmpty(featureId))
			props.setProperty(DittoUtils.DITTO_FEATURE_ID(), featureId);
		
		return props;

	}
	
	public void validate() {
		super.validate();
		
		if (Strings.isNullOrEmpty(endpoint))
			throw new IllegalArgumentException(
					String.format("[%s] The Thing service endpoint must not be empty.", this.getClass().getName()));
		
		/* The current implementation supports a single notification */
		int count = 0;
		if (thingChanges.equals("true"))
			count += 1;
		
		if (featuresChanges.equals("true"))
			count += 1;
		
		if (featureChanges.equals("true"))
			count += 1;
		
		if (liveMessages.equals("true"))
			count += 1;

		if (count == 0)
			throw new IllegalArgumentException(
					String.format("[%s] No thing or feature notification selected.", this.getClass().getName()));

		if (count > 1)
			throw new IllegalArgumentException(
					String.format("[%s] Selecting more than one notification is not supported.", this.getClass().getName()));
			
	}
}
