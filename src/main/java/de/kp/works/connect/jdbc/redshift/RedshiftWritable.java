package de.kp.works.connect.jdbc.redshift;
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

import java.util.List;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.connect.jdbc.JdbcWritable;

public class RedshiftWritable extends JdbcWritable {
	
	public RedshiftWritable(RedshiftConnect connect, StructuredRecord record) {
		this.connect = connect;
		this.record = record;
	}

	/**
	 * Used in map-reduce. Do not remove.
	 */
	public RedshiftWritable() {
	}

	@Override
	public List<String> getColumns(Schema schema) throws Exception {
		
		String primaryKey = connect.getPrimaryKey();
		return RedshiftUtils.getColumns(schema, primaryKey);
		
	}

}
