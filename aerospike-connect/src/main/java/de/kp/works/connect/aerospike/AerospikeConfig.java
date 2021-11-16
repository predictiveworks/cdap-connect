package de.kp.works.connect.aerospike;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.google.common.base.Strings;

import de.kp.works.connect.common.BaseConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

public class AerospikeConfig extends BaseConfig {

    private static final long serialVersionUID = -665126953949777002L;

    @Description("The host of the Aerospike database.")
    @Macro
    public String host;

    @Description("The port of the Aerospike database.")
    @Macro
    public Integer port;

    @Description("The timeout of an Aerospike database connection in milliseconds. Default is 1000.")
    @Macro
    public Integer timeout;

    @Description("The name of the Aerospike namespace used to organize data.")
    @Macro
    public String namespace;

    @Description("The name of the Aerospike set used to organize data.")
    @Macro
    public String setname;

    /*** CREDENTIALS ***/

    @Description("Name of a registered username. Required for authentication.")
    @Macro
    public String user;

    @Description("Password of the registered user. Required for authentication.")
    @Macro
    public String password;

    public AerospikeConfig() {

        port = 3000;
        timeout = 1000;

    }

    public Properties getConfig() {

        Properties props = new Properties();
        props.setProperty(AerospikeUtil.AEROSPIKE_HOST, host);
        props.setProperty(AerospikeUtil.AEROSPIKE_PORT, String.valueOf(port));

        props.setProperty(AerospikeUtil.AEROSPIKE_TIMEOUT, String.valueOf(timeout));

        props.setProperty(AerospikeUtil.AEROSPIKE_NAMESPACE, namespace);
        props.setProperty(AerospikeUtil.AEROSPIKE_SET, setname);

        props.setProperty(AerospikeUtil.AEROSPIKE_USER, user);
        props.setProperty(AerospikeUtil.AEROSPIKE_PASSWORD, password);

        return props;

    }

    public void validate() {
        super.validate();

        if (Strings.isNullOrEmpty(host)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The database host must not be empty.", this.getClass().getName()));
        }

        if (port < 1) {
            throw new IllegalArgumentException(
                    String.format("[%s] The database port must be positive.", this.getClass().getName()));
        }

        if (Strings.isNullOrEmpty(namespace)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The namespace name must not be empty.", this.getClass().getName()));
        }

        if (Strings.isNullOrEmpty(setname)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The set name must not be empty.", this.getClass().getName()));
        }

        if (Strings.isNullOrEmpty(user)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The user name must not be empty.", this.getClass().getName()));
        }

        if (Strings.isNullOrEmpty(password)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The user password must not be empty.", this.getClass().getName()));
        }

    }

    public Schema getSchema() {

        Schema schema = null;
        List<Schema.Field> fields = new ArrayList<>();
        /*
         * Use Aerospike client to execute a dummy query statement to retrieve a single
         * record.
         *
         * The provided credentials are used to create the Aerospike client
         */
        AerospikeClient client = new AerospikeConnect().getClient(getConfig());

        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(setname);

        try (RecordSet recordSet = client.query(null, stmt)) {
            Iterator<KeyRecord> iter = recordSet.iterator();

            if (iter.hasNext()) {

                Record record = iter.next().record;
                Map<String, Object> bins = record.bins;

                for (Map.Entry<String, Object> bin : bins.entrySet()) {
                    fields.add(bin2Field(bin.getKey(), bin.getValue()));
                }

                schema = Schema.recordOf("aerospike", fields);

            }
        } catch (Exception e) {
            schema = Schema.recordOf("aerospike", fields);

        }

        return schema;

    }

    /*
     * A helper method to transform a bin (name, value) into a Schema.Field
     * description
     */
    private Schema.Field bin2Field(String name, Object value) {
        return Schema.Field.of(name, value2Schema(value));
    }

    /*
     * A helper method to infer the data type from the value object of an Aerospike
     * bin
     */
    private Schema value2Schema(Object value) {

        Schema schema = null;
        if (value instanceof Double)
            schema = Schema.of(Schema.Type.DOUBLE);

        else if (value instanceof Float)
            schema = Schema.of(Schema.Type.FLOAT);

        else if (value instanceof Integer)
            schema = Schema.of(Schema.Type.INT);

        else if (value instanceof Long)
            schema = Schema.of(Schema.Type.LONG);
            /*
             * A [Short] type should not occur, referring to Aerospike's data type
             * documentation
             */
        else if (value instanceof Short)
            schema = Schema.of(Schema.Type.LONG);

        else if (value instanceof String)
            schema = Schema.of(Schema.Type.STRING);

        else if (value instanceof Collection<?>) {

            List<Object> values = new ArrayList<>((Collection<?>) value);
            Object head = values.get(0);
            /*
             * Special treatment of Byte Arrays
             */
            if (head instanceof Byte) {
                schema = Schema.of(Schema.Type.BYTES);

            } else {
                schema = Schema.arrayOf(value2Schema(head));

            }

        } else if (value instanceof Map<?, ?>) {

            Map<Object, Object> map = new HashMap<>((Map<?, ?>) value);
            Map.Entry<Object, Object> head = map.entrySet().iterator().next();

            schema = Schema.mapOf(value2Schema(head.getKey()), value2Schema(head.getValue()));

        } else if (value.getClass().isArray()) {

            List<Object> values = new ArrayList<>(Arrays.asList((Object[]) value));
            Object head = values.get(0);
            /*
             * Special treatment of Byte Arrays
             */
            if (head instanceof Byte) {
                schema = Schema.of(Schema.Type.BYTES);

            } else {
                schema = Schema.arrayOf(value2Schema(head));

            }

        }

        return schema;

    }

    public StructuredRecord bins2Record(Map<String, Object> bins, Schema schema) {

        assert schema.getFields() != null;

        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        for (Schema.Field field : schema.getFields()) {

            String fieldName = field.getName();
            Object fieldValue = bins.get(fieldName);

            Schema fieldSchema = field.getSchema();
            builder.set(fieldName, fromBinValue(fieldValue, fieldSchema));

        }

        return builder.build();

    }

    private Object fromBinValue(Object value, Schema schema) {

        switch (schema.getType()) {
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case STRING:
                return value;

            case BYTES:
                return ByteBuffer.wrap((byte[]) value);

            case ARRAY: {

                /* value must be collection */
                Collection<?> collection = (Collection<?>) value;
                List<Object> result = new ArrayList<>(collection.size());

                for (Object element : collection) {
                    /*
                     * Nullable is not relevant here: see schema inference
                     */
                    Schema componentSchema = schema.getComponentSchema();
                    assert componentSchema != null;

                    result.add(fromBinValue(element, componentSchema));

                }

                return result;
            }
            case MAP: {

                /* value mus be a map */
                Map<?, ?> map = (Map<?, ?>) value;
                Map<Object, Object> result = new LinkedHashMap<>(map.size());

                Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();

                assert mapSchema != null;

                Schema keySchema = mapSchema.getKey();
                Schema valueSchema = mapSchema.getValue();

                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    result.put(fromBinValue(entry.getKey(), keySchema), fromBinValue(entry.getValue(), valueSchema));
                }

                return result;

            }
            default:
                throw new IllegalArgumentException(
                        String.format("Data type '%s' is not supported.", schema.getType().name()));
        }

    }
}
