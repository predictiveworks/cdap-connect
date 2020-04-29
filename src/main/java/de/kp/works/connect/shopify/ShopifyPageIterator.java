package de.kp.works.connect.shopify;
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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.Header;
import org.apache.http.HeaderElement;

import de.kp.works.connect.http.HttpResponse;
import de.kp.works.connect.http.page.HttpPage;

public class ShopifyPageIterator implements Iterator<HttpPage>, Closeable {
	
	private static final Pattern nextPattern = Pattern.compile("<(.+)>; rel=next");

	/*
	 * Sample of a response header link:
	 * 
	 * 	Link: "<https://{shop}.myshopify.com/admin/api/{version}/products.json?page_info={page_info}&limit={limit}>; rel={next}, 
	 * <https://{shop}.myshopify.com/admin/api/{version}/products.json?page_info={page_info}&limit={limit}>; rel={previous}"
	 */
	
	public ShopifyPageIterator(ShopifyConfig config) {
	}

	/*
	 * REST endpoints support cursor-based pagination. When you send a request 
	 * to one of these endpoints, the response body returns the first page of 
	 * results, and a response header returns links to the next page and the
	 *  previous page of results (if applicable). 
	 */
	public String getNextPageUrl(HttpResponse response, HttpPage page) {
		
		Header link = response.getFirstHeader("Link");
		
		if (link == null) {
			return null;
		}

		for (HeaderElement headerElement : link.getElements()) {
			
			Matcher matcher = nextPattern.matcher(headerElement.toString());
			if (matcher.matches()) {
				return matcher.group(1);
			}
		
		}

		return null;
		
	}
	
	@Override
	public HttpPage next() {
		return null;
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public void close() throws IOException {
	}
	
}
