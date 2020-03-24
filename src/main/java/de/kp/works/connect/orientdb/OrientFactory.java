package de.kp.works.connect.orientdb;
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

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

public class OrientFactory {

	private OrientGraphFactory db;
	private OrientGraphNoTx conn;
	/*
	 * The url is of the format remote:192.168.163.10 (or any other IP address)
	 */
	public OrientFactory(String url, String user, String password) {
		db = new OrientGraphFactory(url, user, password);
	}
	
	public OrientGraphNoTx getConn() {
		
		conn = db.getNoTx();
		return conn;
		
	}
	
	public void closeConn() {
		db.close();
	}

}
