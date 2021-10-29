package de.kp.works.connect.ignite;
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

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import joptsimple.internal.Strings;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.client.ClientCache;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

public class IgniteCacheWritable implements IgniteWritable {

	private final StructuredRecord record;
	private final Schema schema;
	
	private List<String> values;
	private org.apache.ignite.binary.BinaryObject object;
	
	public IgniteCacheWritable(StructuredRecord record) {
		
		this.record = record;
		
		Schema schema = record.getSchema();
		this.schema =  (schema.isNullable()) ? schema.getNonNullable() : schema;
		
	}

	@Override
	public void write(IgniteContext context, String cacheName, String cacheMode) throws Exception {
		/*
		 * We have to check whether the respective cache exists
		 * as it is not assured that the schema is explicitly
		 * available in the plugin initialization phase
		 */
		if (!context.cacheExists(cacheName))
			context.createCache(cacheName, cacheMode, schema);

		/* Build binary object */
		toBinaryObject(context, cacheName);
		String key = getKey();

		/*
		 * Retrieve cache and write structured record
		 * to the Apache Ignite cache
		 */
		ClientCache<String, org.apache.ignite.binary.BinaryObject> cache = context.getClient().cache(cacheName);
		cache.put(key, object);

	}

	private String getKey() throws Exception {

		String serialized = Strings.join(values, "|");
		return Arrays.toString(MessageDigest.getInstance("MD5").digest(serialized.getBytes()));
		
	}
	
	private void toBinaryObject(IgniteContext context, String cacheName) {
	    
		values = new ArrayList<>();
		
		BinaryObjectBuilder builder = context.getClient().binary().builder(cacheName);

		assert schema.getFields() != null;
		for (Schema.Field field : schema.getFields()) {
			
			String fieldName = field.getName();
			Object value = record.get(fieldName);			
			/*
			 * BYTES are represented as ByteBuffer and this equivalent
			 * with the definition of the cache internal schema;
			 * 
			 *  also in this case nothing has to be done
			 */
			builder.setField(fieldName, value);

			assert value != null;
			values.add(value.toString());
			
		}
		
		object = builder.build();

	}
	
}
