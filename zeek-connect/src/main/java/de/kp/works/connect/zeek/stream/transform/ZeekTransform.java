package de.kp.works.connect.zeek.stream.transform;
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
import de.kp.works.connect.zeek.ZeekUtil;
import de.kp.works.connect.zeek.stream.ZeekConfig;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * [ZeekTransform] is the main class for transforming
 * Works Stream messages originating from Zeek sensor.
 *
 * Different from plain Kafka messages, a pre-defined
 * message schema is expected and used.
 */
public class ZeekTransform implements Function<JavaRDD<String>, JavaRDD<StructuredRecord>> {

    private Schema schema;

    public ZeekTransform(ZeekConfig config) {
    }

    @Override
    public JavaRDD<StructuredRecord> call(JavaRDD<String> input) throws Exception {

        if (input.isEmpty())
            return input.map(new EmptyFunction());

        if (schema == null) {
            schema = getSchema(input.first());
        }

        Function<String, StructuredRecord> logTransform = new LogTransform(schema);
        return input.map(logTransform);

    }

    private Schema getSchema(String event) throws Exception {

        JsonElement jsonElement = JsonParser.parseString(event);

        if (!jsonElement.isJsonObject())
            throw new Exception(
                    String.format("[%s] Zeek events must be JSON objects.", ZeekTransform.class.getName()));

        JsonObject eventObject = jsonElement.getAsJsonObject();
        return ZeekUtil.getSchema(eventObject);

    }

    private static  class EmptyFunction implements Function<String, StructuredRecord> {

        @Override
        public StructuredRecord call(String in) {

            List<Schema.Field> schemaFields = new ArrayList<>();
            Schema schema = Schema.recordOf("emptyOutput", schemaFields);

            StructuredRecord.Builder builder = StructuredRecord.builder(schema);
            return builder.build();

        }

    }

}

