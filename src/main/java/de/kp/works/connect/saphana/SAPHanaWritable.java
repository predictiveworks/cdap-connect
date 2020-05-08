package de.kp.works.connect.saphana;
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.kp.works.connect.jdbc.JdbcWritable;

public class SAPHanaWritable extends JdbcWritable {

	private static final Logger LOG = LoggerFactory.getLogger(SAPHanaWritable.class);

	/**
	 * Used in map-reduce. Do not remove.
	 */
	public SAPHanaWritable() {
	}

	@Override
	public PreparedStatement write(Connection conn, PreparedStatement stmt) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

}
