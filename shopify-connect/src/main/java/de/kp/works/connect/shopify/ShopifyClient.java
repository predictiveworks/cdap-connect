package de.kp.works.connect.shopify;
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

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import javax.net.ssl.SSLContext;

public class ShopifyClient implements Closeable {

	private CloseableHttpClient httpClient;

	public ShopifyClient() {
	}

	/**
	 * Executes HTTP request with parameters configured and returns response. Is
	 * called to load every page by pagination iterator.
	 */
	public CloseableHttpResponse executeHTTP(String uri) throws Exception {

		/*
		 * Lazy initialization. So we are able to initialize the class for different
		 * checks during validations etc.
		 */
		if (httpClient == null) {
			httpClient = createHttpClient();
		}
		/*
		 * Requests to Shopify's REST API are limited GET requests
		 */
		HttpEntityEnclosingRequestBase request = new HttpRequest(URI.create(uri), "GET");
		return httpClient.execute(request);

	}

	@Override
	public void close() throws IOException {

		if (httpClient != null) {
			httpClient.close();
		}
	}

	private CloseableHttpClient createHttpClient() throws Exception {

		String TLS_VERSION = "TLSv1.2";
		SSLContext sslContext = SSLContext.getInstance(TLS_VERSION);
		sslContext.init(null, null, null);

		return HttpClients.custom().setSSLContext(sslContext).setSSLHostnameVerifier(new NoopHostnameVerifier())
				.build();

	}

	/**
	 * This class allows us to send body not only in POST/PUT but also in other
	 * requests.
	 */
	private static class HttpRequest extends HttpEntityEnclosingRequestBase {
		private final String methodName;

		HttpRequest(URI uri, String methodName) {
			super();
			this.setURI(uri);
			this.methodName = methodName;
		}

		@Override
		public String getMethod() {
			return methodName;
		}
	}
}
