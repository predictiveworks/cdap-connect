package de.kp.works.connect.mqtt;
/*
 * Copyright (c) 2020 Dr. Krusche & Partner PartG. All rights reserved.
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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import co.cask.cdap.api.data.format.StructuredRecord;
import de.kp.works.stream.mqtt.*;

public class MqttTransform implements Function<JavaRDD<MqttResult>, JavaRDD<StructuredRecord>> {

	private static final long serialVersionUID = 5511944788990345893L;

	private MqttConfig config;
	
	public MqttTransform(MqttConfig config) {
		this.config = config;
	}
	
	@Override
	public JavaRDD<StructuredRecord> call(JavaRDD<MqttResult> input) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
