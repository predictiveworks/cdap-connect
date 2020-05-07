package de.kp.works.connect.netflow.v5;
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

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import de.kp.works.connect.netflow.NetflowMessage;

public class Netflow5Message extends NetflowMessage {

	private static final String FIELD_COUNT = "count";

	private static final String FIELD_ENGINE_ID = "engine_id";
	private static final String FIELD_ENGINE_TYPE = "engine_type";
	
	private static final String FIELD_FLOW_SEQ = "flow_seq";
	private static final String FIELD_ID = "id";

	private static final String FIELD_PACKET_ID = "packet_id";
	private static final String FIELD_PACKET_LENGTH = "packet_length";

	private static final String FIELD_SECONDS = "seconds";
	private static final String FIELD_NANOS = "nanos";
	
	private static final String FIELD_SAMPLING_INT = "sampling_interval";
	private static final String FIELD_SAMPLING_MODE = "sampling_mode";
	private static final String FIELD_SAMPLING_RAW = "sampling_raw";
	
	private static final String FIELD_SNMP_INPUT = "snmp_input";
	private static final String FIELD_SNMP_OUTPUT = "snmp_output";

	private static final String FIELD_TIMESTAMP = "timestamp";
	private static final String FIELD_UPTIME = "uptime";	
	
	private static final String FIELD_SENDER = "sender";
	private static final String FIELD_READER_ID = "reader_id";

	private static final String FIELD_FIRST = "first";
	private static final String FIELD_FIRST_RAW = "first_raw";
	
	private static final String FIELD_LAST = "last";
	private static final String FIELD_LAST_RAW = "last_raw";

	private static final String FIELD_SRC_ADDR = "src_addr";
	private static final String FIELD_DST_ADDR = "dst_addr";

	private static final String FIELD_NEXT_HOP = "next_hop";

	private static final String FIELD_SRC_ADDR_S = "src_ip";
	private static final String FIELD_DST_ADDR_S = "dst_ip";
	private static final String FIELD_NEXT_HOP_S = "next_hop_ip";
	
	private static final String FIELD_SRC_PORT = "src_port";
	private static final String FIELD_DST_PORT = "dst_port";
	
	private static final String FIELD_TCP_FLAGS = "tcp_flags";

	private static final String FIELD_PROTO = "proto";
	private static final String FIELD_TOS = "tos";
	
	private static final String FIELD_SRC_AS = "src_as";
	private static final String FIELD_DST_AS = "dst_as";

	private static final String FIELD_SRC_MASK = "src_mask";
	private static final String FIELD_DST_MASK = "dst_mask";

	private static final String FIELD_PACKETS = "dPkts";
	private static final String FIELD_DOCTETS = "dOctets";
	
	private JsonObject message = new JsonObject();

	@Override
	public StructuredRecord toRecord() throws Exception {

		/* Retrieve structured record */
		String json = message.toString();
		return StructuredRecordStringConverter.fromJsonString(json, getSchema());

	}

	public void setCount(Integer value) {
		message.addProperty(FIELD_COUNT, value);
	}

	public void setDstAddr(Integer value) {
		message.addProperty(FIELD_DST_ADDR, value);
	}

	public void setDstPort(Integer value) {
		message.addProperty(FIELD_DST_PORT, value);
	}

	public void setDstAddrAsIP(String value) {
		message.addProperty(FIELD_DST_ADDR_S, value);
	}

	public void setDstAs(Integer value) {
		message.addProperty(FIELD_DST_AS, value);
	}

	public void setDstMask(Integer value) {
		message.addProperty(FIELD_DST_MASK, value);
	}

	public void setDOctets(Long value) {
		message.addProperty(FIELD_DOCTETS, value);
	}

	public void setNanos(Long value) {
		message.addProperty(FIELD_NANOS, value);
	}

	public void setEngineId(Integer value) {
		message.addProperty(FIELD_ENGINE_ID, value);
	}

	public void setEngineType(Integer value) {
		message.addProperty(FIELD_ENGINE_TYPE, value);
	}

	public void setFirst(Long value) {
		message.addProperty(FIELD_FIRST, value);
	}

	public void setFirstRaw(Long value) {
		message.addProperty(FIELD_FIRST_RAW, value);
	}

	public void setFlowSeq(Long value) {
		message.addProperty(FIELD_FLOW_SEQ, value);
	}

	public void setId(String value) {
		message.addProperty(FIELD_ID, value);
	}

	public void setLast(Long value) {
		message.addProperty(FIELD_LAST, value);
	}

	public void setLastRaw(Long value) {
		message.addProperty(FIELD_LAST_RAW, value);
	}

	public void setNextHop(Integer value) {
		message.addProperty(FIELD_NEXT_HOP, value);
	}

	public void setNextHopAsIP(String value) {
		message.addProperty(FIELD_NEXT_HOP_S, value);
	}

	public void setPacketId(String value) {
		message.addProperty(FIELD_PACKET_ID, value);
	}

	public void setPacketLength(Integer value) {
		message.addProperty(FIELD_PACKET_LENGTH, value);
	}

	public void setPackets(Long value) {
		message.addProperty(FIELD_PACKETS, value);
	}

	public void setProto(Integer value) {
		message.addProperty(FIELD_PROTO, value);
	}

	public void setReaderId(String value) {
		message.addProperty(FIELD_READER_ID, value);
	}

	public void setSeconds(Long value) {
		message.addProperty(FIELD_SECONDS, value);
	}

	public void setSamplingInt(Integer value) {
		message.addProperty(FIELD_SAMPLING_INT, value);
	}

	public void setSamplingMode(Integer value) {
		message.addProperty(FIELD_SAMPLING_MODE, value);
	}

	public void setSamplingRaw(Integer value) {
		message.addProperty(FIELD_SAMPLING_RAW, value);
	}

	public void setSender(String value) {
		message.addProperty(FIELD_SENDER, value);
	}

	public void setSnmpInput(Integer value) {
		message.addProperty(FIELD_SNMP_INPUT, value);
	}

	public void setSnmpOutput(Integer value) {
		message.addProperty(FIELD_SNMP_OUTPUT, value);
	}

	public void setSrcAddr(Integer value) {
		message.addProperty(FIELD_SRC_ADDR, value);
	}

	public void setSrcPort(Integer value) {
		message.addProperty(FIELD_SRC_PORT, value);
	}

	public void setSrcAddrAsIP(String value) {
		message.addProperty(FIELD_SRC_ADDR_S, value);
	}

	public void setSrcAs(Integer value) {
		message.addProperty(FIELD_SRC_AS, value);
	}

	public void setSrcMask(Integer value) {
		message.addProperty(FIELD_SRC_MASK, value);
	}

	public void setTcpFlags(Integer value) {
		message.addProperty(FIELD_TCP_FLAGS, value);
	}

	public void setTimestamp(Long value) {
		message.addProperty(FIELD_TIMESTAMP, value);
	}

	public void setTos(Integer value) {
		message.addProperty(FIELD_TOS, value);
	}

	public void setUptime(Long value) {
		message.addProperty(FIELD_UPTIME, value);
	}

	private Schema getSchema() {

		List<Schema.Field> fields = new ArrayList<>();

		/* Number of flows that are exported in this packet (1-30) */
		fields.add(Schema.Field.of(FIELD_COUNT, Schema.of(Schema.Type.INT)));
		
		/* Current count of seconds since 0000 Coordinated Universal Time 1970 */
		fields.add(Schema.Field.of(FIELD_SECONDS, Schema.of(Schema.Type.LONG)));
		
		/* Residual nanoseconds since 0000 Coordinated Universal Time 1970 */
		fields.add(Schema.Field.of(FIELD_NANOS, Schema.of(Schema.Type.LONG)));

		fields.add(Schema.Field.of(FIELD_PACKET_ID, Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of(FIELD_PACKET_LENGTH, Schema.of(Schema.Type.INT)));

		/* SNMP index of input interface */
		fields.add(Schema.Field.of(FIELD_SNMP_INPUT, Schema.of(Schema.Type.INT)));
		
		/* SNMP index of output interface */
		fields.add(Schema.Field.of(FIELD_SNMP_OUTPUT, Schema.of(Schema.Type.INT)));

		/* Current time in milliseconds since the export device started */
		fields.add(Schema.Field.of(FIELD_TIMESTAMP, Schema.of(Schema.Type.LONG)));
		fields.add(Schema.Field.of(FIELD_UPTIME, Schema.of(Schema.Type.LONG)));

		/* Slot number of the flow-switching engine */
		fields.add(Schema.Field.of(FIELD_ENGINE_ID, Schema.of(Schema.Type.INT)));
		
		/* Type of flow-switching engine */
		fields.add(Schema.Field.of(FIELD_ENGINE_TYPE, Schema.of(Schema.Type.INT)));
		
		/* Sequence counter of total flows seen */
		fields.add(Schema.Field.of(FIELD_FLOW_SEQ, Schema.of(Schema.Type.LONG)));

		/* First two bits hold the sampling mode; remaining 14 bits hold 
		 * value of sampling interval 
		 */		
		fields.add(Schema.Field.of(FIELD_SAMPLING_INT, Schema.of(Schema.Type.INT)));
		
		
		fields.add(Schema.Field.of(FIELD_SAMPLING_MODE, Schema.of(Schema.Type.INT)));
		fields.add(Schema.Field.of(FIELD_SAMPLING_RAW, Schema.of(Schema.Type.INT)));
		
		fields.add(Schema.Field.of(FIELD_SENDER, Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of(FIELD_READER_ID, Schema.of(Schema.Type.STRING)));

		/* SysUptime at start of flow */
		fields.add(Schema.Field.of(FIELD_FIRST, Schema.of(Schema.Type.LONG)));
		fields.add(Schema.Field.of(FIELD_FIRST_RAW, Schema.of(Schema.Type.LONG)));

		/* SysUptime at the time the last packet of the flow was received */
		fields.add(Schema.Field.of(FIELD_LAST, Schema.of(Schema.Type.LONG)));
		fields.add(Schema.Field.of(FIELD_LAST_RAW, Schema.of(Schema.Type.LONG)));

		fields.add(Schema.Field.of(FIELD_ID, Schema.of(Schema.Type.STRING)));
		
		/* Source IP Address */
		fields.add(Schema.Field.of(FIELD_SRC_ADDR, Schema.of(Schema.Type.INT)));
		fields.add(Schema.Field.of(FIELD_SRC_ADDR_S, Schema.of(Schema.Type.STRING)));

		/* Destination IP Address */
		fields.add(Schema.Field.of(FIELD_DST_ADDR, Schema.of(Schema.Type.INT)));
		fields.add(Schema.Field.of(FIELD_DST_ADDR_S, Schema.of(Schema.Type.STRING)));

		/* IP address of nexthop router */
		fields.add(Schema.Field.of(FIELD_NEXT_HOP, Schema.of(Schema.Type.INT)));
		fields.add(Schema.Field.of(FIELD_NEXT_HOP_S, Schema.of(Schema.Type.STRING)));

		/* TCP/UDP source port number or equivalent */
		fields.add(Schema.Field.of(FIELD_SRC_PORT, Schema.of(Schema.Type.INT)));
		
		/* TCP/UDP destination port number or equivalent */
		fields.add(Schema.Field.of(FIELD_DST_PORT, Schema.of(Schema.Type.INT)));

		/* Cumulative OR of TCP flags */
		fields.add(Schema.Field.of(FIELD_TCP_FLAGS, Schema.of(Schema.Type.INT)));

		/* IP protocol type (for example, TCP = 6; UDP = 17) */
		fields.add(Schema.Field.of(FIELD_PROTO, Schema.of(Schema.Type.INT)));
		
		/* IP type of service (ToS) */
		fields.add(Schema.Field.of(FIELD_TOS, Schema.of(Schema.Type.INT)));

		/* Autonomous system number of the source, either origin or peer */
		fields.add(Schema.Field.of(FIELD_SRC_AS, Schema.of(Schema.Type.INT)));
		
		/* Autonomous system number of the destination, either origin or peer */
		fields.add(Schema.Field.of(FIELD_DST_AS, Schema.of(Schema.Type.INT)));

		/* Source address prefix mask bits */
		fields.add(Schema.Field.of(FIELD_SRC_MASK, Schema.of(Schema.Type.INT)));
		
		/* Destination address prefix mask bits */
		fields.add(Schema.Field.of(FIELD_DST_MASK, Schema.of(Schema.Type.INT)));
		
		/* Packets in the flow */
		fields.add(Schema.Field.of(FIELD_PACKETS, Schema.of(Schema.Type.LONG)));
		
		/* Total number of Layer 3 bytes in the packets of the flow */
		fields.add(Schema.Field.of(FIELD_DOCTETS, Schema.of(Schema.Type.LONG)));

		Schema schema = Schema.recordOf("netflow5Schema", fields);
		return schema;

	}
}
