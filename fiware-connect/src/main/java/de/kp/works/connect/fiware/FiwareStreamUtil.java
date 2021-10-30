package de.kp.works.connect.fiware;
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

import de.kp.works.connect.fiware.transform.FiwareTransform;
import de.kp.works.stream.fiware.FiwareStream;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.List;
import java.util.Properties;

public class FiwareStreamUtil {
	/*
	 * mqttSecure is currently unused
	 */
	public static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(
			StreamingContext context, FiwareConfig zeekConfig, List<SecureStoreMetadata> mqttSecure) {

		Properties properties = zeekConfig.toProperties();
		StorageLevel storageLevel = StorageLevel.MEMORY_ONLY_SER();

		return FiwareStream.createDirectStream(context.getSparkStreamingContext(), properties, storageLevel)
				.transform(new FiwareTransform(zeekConfig));

	}

	private FiwareStreamUtil() {
		// no-op
	}

}
