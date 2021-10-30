package de.kp.works.connect.things.stream;
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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;

import java.util.ArrayList;
import java.util.List;

public class ThingsUtil {

    static String className = ThingsUtil.class.getName();

    public static Schema getSchema(JsonObject jsonObject) {

        List<Schema.Field> fields = new ArrayList<>();

        return Schema.recordOf("ThingsSchema", fields);

    }

    public static StructuredRecord toRecord(String event, Schema schema) throws Exception {

        JsonElement jsonElement = JsonParser.parseString(event);
        if (!jsonElement.isJsonObject())
            throw new Exception(
                    String.format("[%s] Things events must be JSON objects.", className));

        JsonObject eventObj = jsonElement.getAsJsonObject();
        JsonObject recordObj = new JsonObject();

        /* Retrieve structured record */
        String json = recordObj.toString();
        return StructuredRecordStringConverter.fromJsonString(json, schema);

    }

}
