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

import java.util.List;

import com.google.common.collect.Lists;

import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.connect.jdbc.JdbcUtils;

public class SAPHanaUtils extends JdbcUtils {
	
	private static final long serialVersionUID = -6304203493395042649L;
	
	private static final Character ESCAPE_CHAR = '"';

	public static List<String> getColumns(Schema schema, String primaryKey) throws Exception {

		List<String> columns = Lists.newArrayList();
		for (Schema.Field field : schema.getFields()) {
			
			String fieldName = field.getName();
			String fieldType = getSqlType(field.getSchema());

			String column = null;
			if (fieldName.equals(primaryKey)) {
				/*
				 * Other than in UPSERT statements, SAP Hana expects
				 * escaped field names in CREATE ROW TABLE statements
				 */
				fieldName = ESCAPE_CHAR + fieldName + ESCAPE_CHAR;
				String.format("%s %s PRIMARY KEY", fieldName, fieldType);
				
			} else {
				
				fieldName = ESCAPE_CHAR + fieldName + ESCAPE_CHAR;
				String.format("%s %s", fieldName, fieldType);

			}
					
					
			columns.add(column);
			
		}

		return columns;
	}

/*
* CREATE COLUMN TABLE "CODEJAMMER"."STORE_ADDRESS" (
ID bigint not null primary key ,
"STREETNUMBER" INTEGER CS_INT,
"STREET" NVARCHAR(200),
"LOCALITY" NVARCHAR(200),
"STATE" NVARCHAR(200),
"COUNTRY" NVARCHAR(200)) UNLOAD PRIORITY 5 AUTO MERGE ;

insert into "CODEJAMMER"."STORE_ADDRESS" (ID,STREETNUMBER,STREET,LOCALITY,STATE,COUNTRY)  values(1,555,'Madison Ave','New York','NY','America');
insert into "CODEJAMMER"."STORE_ADDRESS" (ID,STREETNUMBER,STREET,LOCALITY,STATE,COUNTRY)  values(2,95,'Morten Street','New York','NY','USA');
insert into "CODEJAMMER"."STORE_ADDRESS" (ID,STREETNUMBER,STREET,LOCALITY,STATE,COUNTRY)  values(3,2395,'Broadway Street','New York','NY','USA');   		
*/

	private static String getSqlType(Schema schema) {
		return null;
	}
}
