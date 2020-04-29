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

import java.io.IOException;

import de.kp.works.connect.http.HttpResponse;
import de.kp.works.connect.http.page.HttpEntry;
import de.kp.works.connect.http.page.HttpPage;

public class ShopifyPage extends HttpPage {

	private final ShopifyConfig config;

	protected ShopifyPage(ShopifyConfig config, HttpResponse httpResponse) {
		super(httpResponse);

		this.config = config;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public HttpEntry next() {
		// TODO Auto-generated method stub
		return null;
	}

}
