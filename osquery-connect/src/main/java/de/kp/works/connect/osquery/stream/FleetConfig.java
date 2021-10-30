package de.kp.works.connect.osquery.stream;
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

import com.google.common.base.Strings;
import de.kp.works.connect.common.BaseConfig;
import de.kp.works.stream.fleet.FleetNames;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import javax.annotation.Nullable;
import java.util.Properties;

public class FleetConfig extends BaseConfig {

	@Description("The file system folder used by the Fleet platform to persist log files.")
	@Macro
	public String fleetFolder;

	@Description("The log file extension used by the Fleet platform . Default value is 'log'.")
	@Macro
	@Nullable
	public String fleetExtension;

	@Description("The buffer size used to cache Fleet log entries. Default value is '1000'.")
	@Macro
	@Nullable
	public String fleetBufferSize;

	@Description("The maximum number of bytes of a Fleet log entry. Default value is '8192'.")
	@Macro
	@Nullable
	public String fleetLineSize;

	@Description("The number of threads used to process Fleet log entries. Default value is '1'.")
	@Macro
	@Nullable
	public String fleetNumThreads;

	@Description("The polling interval in seconds to retrieve Fleet log entries. Default value is '1'.")
	@Macro
	@Nullable
	public String fleetPolling;

	public void validate() {

		String className = this.getClass().getName();

		if (Strings.isNullOrEmpty(fleetFolder)) {
			throw new IllegalArgumentException(
					String.format("[%s] The Fleet log folder must not be empty.", className));
		}

	}

	public Properties toProperties() {

		Properties props = new Properties();
		props.setProperty(FleetNames.LOG_FOLDER(), fleetFolder);

		if (!Strings.isNullOrEmpty(fleetExtension)) {
			props.setProperty(FleetNames.LOG_POSTFIX(), fleetExtension);
		}

		if (!Strings.isNullOrEmpty(fleetBufferSize)) {
			props.setProperty(FleetNames.MAX_BUFFER_SIZE(), fleetBufferSize);
		}

		if (!Strings.isNullOrEmpty(fleetLineSize)) {
			props.setProperty(FleetNames.MAX_LINE_SIZE(), fleetLineSize);
		}

		if (!Strings.isNullOrEmpty(fleetNumThreads)) {
			props.setProperty(FleetNames.NUM_THREADS(), fleetNumThreads);
		}

		if (!Strings.isNullOrEmpty(fleetPolling)) {
			props.setProperty(FleetNames.POLLING_INTERVAL(), fleetPolling);
		}

		return props;
	}
}
