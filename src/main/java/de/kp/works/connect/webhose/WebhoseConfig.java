package de.kp.works.connect.webhose;
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

import java.util.stream.Stream;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import de.kp.works.connect.BaseConfig;

public class WebhoseConfig extends BaseConfig {

	private static final long serialVersionUID = 1314113942758850774L;

	@Description("The message format of the Webhose data feed. This determines the Webhose "
			+ "API to be selected for data retrieval.")
	@Macro
	private String messageFormat;

	public void validate() {
		super.validate();
	}
	
	public WebhoseFormat getMessageFormat() {
		
		Class<WebhoseFormat> enumClazz = WebhoseFormat.class;

		return Stream.of(enumClazz.getEnumConstants())
				.filter(keyType -> keyType.getValue().equalsIgnoreCase(messageFormat)).findAny()
				.orElseThrow(() -> new IllegalArgumentException(
						String.format("Unsupported value for 'format': '%s'", messageFormat)));

	}
	
}
