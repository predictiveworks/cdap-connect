package de.kp.works.connect.snowflake;
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

public enum SnowflakeRegions {
    /* AWS */
    US_WEST_2("us-west-2"),
    US_EAST_1("us-east-1"),
    CA_CENTRAL_1("ca-central-1"),
    EU_WEST_1("eu-west-1"),
    EU_CENTRAL_1("eu-central-1"),
    AP_SOUTHEAST_1("ap-southeast-1"),
    AP_SOUTHEAST_2("ap-southeast-2"),
    /* AZURE */
    EAST_US_2_AZURE("east-us-2.azure"),
    CANADA_CENTRAL_AZURE("canada-central.azure"),
    WEST_EUROPE_AZURE("west-europe.azure"),
    AUSTRALIA_EAST_AZURE("australia-east.azure"),
    SOUTHEAST_ASIA_AZURE("southeast-asia.azure");

    private final String region;

    SnowflakeRegions(String region) {
        this.region = region;
    }

    public String getRegion() {
        return region;

    }
}
