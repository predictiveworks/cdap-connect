package de.kp.works.connect.ditto;
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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

public class MessageTransform implements Function<String, StructuredRecord> {

    private static final long serialVersionUID = -8859251744707152433L;

    @Override
    public StructuredRecord call(String in) {

        JsonObject json = new Gson().fromJson(in, JsonObject.class);

        Schema schema = buildSchema();
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);

        List<Schema.Field> fields = schema.getFields();

        assert fields != null;
        for (Schema.Field field : fields) {

            String fieldName = field.getName();
            Object fieldValue;

            if (fieldName.equals("timestamp"))
                fieldValue = json.get(fieldName).getAsLong();

            else
                fieldValue = json.get(fieldName).getAsString();

            builder.set(fieldName, fieldValue);

        }

        return builder.build();

    }

    /**
     * A message schema is static (in contrast to change schemas)
     * and can be predefined
     */
    private Schema buildSchema() {

        List<Schema.Field> schemaFields = new ArrayList<>();

        Schema.Field timestamp = Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG));
        schemaFields.add(timestamp);

        Schema.Field name = Schema.Field.of("name", Schema.of(Schema.Type.STRING));
        schemaFields.add(name);

        Schema.Field namespace = Schema.Field.of("namespace", Schema.of(Schema.Type.STRING));
        schemaFields.add(namespace);

        Schema.Field subject = Schema.Field.of("subject", Schema.of(Schema.Type.STRING));
        schemaFields.add(subject);

        Schema.Field payload = Schema.Field.of("payload", Schema.of(Schema.Type.STRING));
        schemaFields.add(payload);

        return Schema.recordOf("LiveMessage", schemaFields);

    }

}

