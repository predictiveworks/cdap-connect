package de.kp.works.connect.common.http;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.base.Strings;
import de.kp.works.connect.common.http.core.ValuedEnum;

public class ConfigUtil {

	public static void assertIsSet(Object propertyValue, String propertyName, String reason) {

		if (propertyValue == null) {
			throw new IllegalArgumentException(
					String.format("Property '%s' must be set, since %s", propertyName, reason));
		}
	}

	public static void assertIsNotSet(Object propertyValue, String propertyName, String reason) {
		if (propertyValue != null) {
			throw new IllegalArgumentException(
					String.format("Property '%s' must not be set, since %s", propertyName, reason));
		}
	}

	public static <T extends ValuedEnum> T getEnumValueByString(Class<T> enumClass, String stringValue,
																String propertyName) {

		return Stream.of(enumClass.getEnumConstants())
				.filter(keyType -> keyType.getValue().equalsIgnoreCase(stringValue)).findAny()
				.orElseThrow(() -> new IllegalArgumentException(
						String.format("Unsupported value for '%s': '%s'", propertyName, stringValue)));

	}

	public static Long toLong(String value, String propertyName) {
		
		if (Strings.isNullOrEmpty(value)) {
			return null;
		}

		try {
			return Long.parseLong(value);
			
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException(String.format("Unsupported value for '%s': '%s'", propertyName, value));
		}
	}

	public static List<String> getListFromString(String value) {

		if (Strings.isNullOrEmpty(value)) {
			return Collections.emptyList();
		}

		return Arrays.asList(value.split(","));

	}

	public static Map<String, String> getMapFromKeyValueString(String keyValueString) {

		Map<String, String> result = new LinkedHashMap<>();

		if (Strings.isNullOrEmpty(keyValueString)) {
			return result;
		}

		String[] mappings = keyValueString.split(",");
		for (String map : mappings) {
			String[] columns = map.split(":");
			result.put(columns[0], columns[1]);
		}

		return result;

	}

}
