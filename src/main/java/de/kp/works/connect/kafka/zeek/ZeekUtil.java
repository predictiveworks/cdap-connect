package de.kp.works.connect.kafka.zeek;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.Field;
import io.cdap.cdap.format.StructuredRecordStringConverter;

/*
 * [ZeekUtil] transforms [String] messages representing JSON objects
 * into [StructuredRecord]. Optional fields are mapped onto NULLABLE
 * fields, which leads to 'null' values for missing values.
 * 
 * The current implementation takes those fields into account where
 * Zeek writes a record field to the log stream (parameter &log)
 */
public class ZeekUtil implements java.io.Serializable {
	
	private static final long serialVersionUID = 5866766866321904962L;
	private static Gson GSON = new Gson();
	/*
	 * capture_loss (&log)
	 * 
	 * This logs evidence regarding the degree to which the packet capture process suffers 
	 * from measurement loss. The loss could be due to overload on the host or NIC performing 
	 * the packet capture or it could even be beyond the host. If you are capturing from a 
	 * switch with a SPAN port, it’s very possible that the switch itself could be overloaded 
	 * and dropping packets. 
	 * 
	 * Reported loss is computed in terms of the number of “gap events” (ACKs for a sequence 
	 * number that’s above a gap).
	 * 
	 * {
	 * 	"ts":1568132368.465338,
	 * 	"ts_delta":32.282249,
	 * 	"peer":"bro",
	 * 	"gaps":0,
	 * 	"acks":206,
	 * 	"percent_lost":0.0
	 * }
	 */
	public static StructuredRecord fromCaptureLoss(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceInterval(newObject, "ts_delta");
		
		/* Retrieve structured record */
		String json = newObject.toString();
		return StructuredRecordStringConverter.fromJsonString(json, capture_loss());
		
	}
	
	private static Schema capture_loss() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the measurement occurred. The original
		 * data type is a double and refers to seconds
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* ts_delta: The time delay between this measurement and the last. 
		 * The original data type is a double and refers to seconds
		 */
		fields.add(Schema.Field.of("ts_delta", Schema.of(Schema.Type.LONG)));
		
		/* peer: In the event that there are multiple Zeek instances logging 
		 * to the same host, this distinguishes each peer with its individual 
		 * name. 
		 */
		fields.add(Schema.Field.of("peer", Schema.of(Schema.Type.STRING)));
		
		/* count: Number of missed ACKs from the previous measurement interval. 
		 */
		fields.add(Schema.Field.of("count", Schema.of(Schema.Type.INT)));
		
		/* acks: Total number of ACKs seen in the previous measurement interval.
		 */
		fields.add(Schema.Field.of("acks", Schema.of(Schema.Type.INT)));
		
		/* percent_lost: Percentage of ACKs seen where the data being ACKed wasn’t seen.
		 */
		fields.add(Schema.Field.of("percent_lost", Schema.of(Schema.Type.DOUBLE)));
			
		Schema schema = Schema.recordOf("capture_log", fields);
		return schema;
		
	}
	/*
	 * 
	 * connection (&log)
	 * 
	 * This logs the tracking/logging of general information regarding TCP, UDP, and ICMP traffic. 
	 * For UDP and ICMP, “connections” are to be interpreted using flow semantics (sequence of
	 * packets from a source host/port to a destination host/port). Further, ICMP “ports” are to
	 * be interpreted as the source port meaning the ICMP message type and the destination port 
	 * being the ICMP message code.
	 * 
	 * {
	 * 	"ts":1547188415.857497,
	 * 	"uid":"CAcJw21BbVedgFnYH3",
	 * 	"id.orig_h":"192.168.86.167",
	 * 	"id.orig_p":38339,
	 * 	"id.resp_h":"192.168.86.1",
	 * 	"id.resp_p":53,
	 * 	"proto":"udp",
	 * 	"service":"dns",
	 * 	"duration":0.076967,
	 * 	"orig_bytes":75,
	 * 	"resp_bytes":178,
	 * 	"conn_state":"SF",
	 * 	"local_orig":true,
	 * 	"local_resp":true,
	 * 	"missed_bytes":0,
	 * 	"history":"Dd",
	 * 	"orig_pkts":1,
	 * 	"orig_ip_bytes":103,
	 * 	"resp_pkts":1,
	 * 	"resp_ip_bytes":206,
	 * 	"tunnel_parents":[]
	 * }
	 */

	public static StructuredRecord fromConnection(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceInterval(newObject, "duration");

		newObject = replaceConnId(newObject);

		newObject = replaceName(newObject, "source_bytes", "orig_bytes");
		newObject = replaceName(newObject, "destination_bytes", "resp_bytes");

		newObject = replaceName(newObject, "source_local", "local_orig");
		newObject = replaceName(newObject, "destination_local", "local_resp");

		newObject = replaceName(newObject, "source_pkts", "orig_pkts");
		newObject = replaceName(newObject, "source_ip_bytes", "orig_ip_bytes");

		newObject = replaceName(newObject, "destination_pkts", "resp_pkts");
		newObject = replaceName(newObject, "destination_ip_bytes", "resp_ip_bytes");

		newObject = replaceName(newObject, "source_l2_addr", "orig_l2_addr");
		newObject = replaceName(newObject, "destination_l2_addr", "resp_l2_addr");
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, connection());
		
	}
		
	private static Schema connection() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the measurement occurred. The original
		 * data type is a double and refers to seconds
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id 
		 */
		fields.addAll(conn_id());
		
		/* proto: A connection’s transport-layer protocol. Supported values are
		 * unknown_transport, tcp, udp, icmp.
		 */
		fields.add(Schema.Field.of("proto", Schema.of(Schema.Type.STRING)));
		
		/* service: An identification of an application protocol being sent over 
		 * the connection. 
		 */
		fields.add(Schema.Field.of("service", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* duration: A temporal type representing a relative time. How long the connection 
		 * lasted. For 3-way or 4-way connection tear-downs, this will not include the final 
		 * ACK. The original data type is a double and refers to seconds
		 */
		fields.add(Schema.Field.of("duration", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* orig_bytes: The number of payload bytes the originator sent. For TCP this is taken 
		 * from sequence numbers and might be inaccurate (e.g., due to large connections). 
		 */
		fields.add(Schema.Field.of("source_bytes", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* resp_bytes: The number of payload bytes the responder sent. See orig_bytes. 
		 */
		fields.add(Schema.Field.of("destination_bytes", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* conn_state: Possible conn_state values.
		 */
		fields.add(Schema.Field.of("conn_state", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* local_orig: If the connection is originated locally, this value will be T. 
		 * If it was originated remotely it will be F.
		 */
		fields.add(Schema.Field.of("source_local", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* local_resp: If the connection is responded to locally, this value will be T. If it was 
		 * responded to remotely it will be F. 
		 */
		fields.add(Schema.Field.of("destination_local", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* missed_bytes: Indicates the number of bytes missed in content gaps, which is representative 
		 * of packet loss. A value other than zero will normally cause protocol analysis to fail but 
		 * some analysis may have been completed prior to the packet loss. 
		 */
		fields.add(Schema.Field.of("missed_bytes", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* history: Records the state history of connections as a string of letters. 
		 */
		fields.add(Schema.Field.of("history", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* orig_pkts: Number of packets that the originator sent. 
		 */
		fields.add(Schema.Field.of("source_pkts", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* orig_ip_bytes: Number of IP level bytes that the originator sent (as seen on the wire, 
		 * taken from the IP total_length header field).  
		 */
		fields.add(Schema.Field.of("source_ip_bytes", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* resp_pkts: Number of packets that the responder sent. 
		 */
		fields.add(Schema.Field.of("destination_pkts", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* resp_ip_bytes: Number of IP level bytes that the responder sent (as seen on the wire, 
		 * taken from the IP total_length header field).  
		 */
		fields.add(Schema.Field.of("destination_ip_bytes", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* tunnel_parents: If this connection was over a tunnel, indicate the uid values for any 
		 * encapsulating parent connections used over the lifetime of this inner connection.  
		 */
		fields.add(Schema.Field.of("tunnel_parents", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* orig_l2_addr: Link-layer address of the originator, if available.  
		 */
		fields.add(Schema.Field.of("source_l2_addr", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* resp_l2_addr: Link-layer address of the responder, if available. 
		 */
		fields.add(Schema.Field.of("destination_l2_addr", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* vlan: The outer VLAN for this connection, if applicable.
		 */
		fields.add(Schema.Field.of("vlan", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* inner_vlan: The inner VLAN for this connection, if applicable.
		 */
		fields.add(Schema.Field.of("inner_vlan", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* speculative_service: Protocol that was determined by a matching signature after the beginning 
		 * of a connection. In this situation no analyzer can be attached and hence the data cannot be 
		 * analyzed nor the protocol can be confirmed.
		 */
		fields.add(Schema.Field.of("speculative_service", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		Schema schema = Schema.recordOf("connection_log", fields);
		return schema;
	
	}
	/*
	 * dce_rpc (&log)
	 * 
	 * {
	 * 	"ts":1361916332.298338,
	 * 	"uid":"CsNHVHa1lzFtvJzT8",
	 * 	"id.orig_h":"172.16.133.6",
	 * 	"id.orig_p":1728,
	 * 	"id.resp_h":"172.16.128.202",
	 * 	"id.resp_p":445,"rtt":0.09211,
	 * 	"named_pipe":"\u005cPIPE\u005cbrowser",
	 * 	"endpoint":"browser",
	 * 	"operation":"BrowserrQueryOtherDomains"
	 * }
	 */
	public static StructuredRecord fromDceRpc(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceInterval(newObject, "rtt");

		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, dce_rpc());
		
	}
	
	private static Schema dce_rpc() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the event happened.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* rtt: Round trip time from the request to the response. If either the 
		 * request or response wasn’t seen, this will be null.
		 */
		fields.add(Schema.Field.of("rtt", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* named_pipe: Remote pipe name.
		 */
		fields.add(Schema.Field.of("named_pipe", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* endpoint: Endpoint name looked up from the uuid.
		 */
		fields.add(Schema.Field.of("endpoint", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* operation: Operation seen in the call.
		 */
		fields.add(Schema.Field.of("operation", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		Schema schema = Schema.recordOf("dce_rpc_log", fields);
		return schema;

	}
	/*
	 * dhcp (&log)
	 * 
	 * {
	 * 	"ts":1476605498.771847,
	 * 	"uids":["CmWOt6VWaNGqXYcH6","CLObLo4YHn0u23Tp8a"],
	 * 	"client_addr":"192.168.199.132",
	 * 	"server_addr":"192.168.199.254",
	 * 	"mac":"00:0c:29:03:df:ad",
	 * 	"host_name":"DESKTOP-2AEFM7G",
	 * 	"client_fqdn":"DESKTOP-2AEFM7G",
	 * 	"domain":"localdomain",
	 * 	"requested_addr":"192.168.199.132",
	 * 	"assigned_addr":"192.168.199.132",
	 * 	"lease_time":1800.0,
	 * 	"msg_types":["REQUEST","ACK"],
	 * 	"duration":0.000161
	 * }
	 */
	public static StructuredRecord fromDhcp(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceInterval(newObject, "lease_time");
		
		newObject = replaceInterval(newObject, "duration");
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, dhcp());

	}
	
	private static Schema dhcp() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: The earliest time at which a DHCP message over the associated 
		 * connection is observed.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uids: A series of unique identifiers of the connections over which 
		 * DHCP is occurring. This behavior with multiple connections is unique 
		 * to DHCP because of the way it uses broadcast packets on local networks.
		 */
		fields.add(Schema.Field.of("uids", Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		
		/* client_addr: IP address of the client. If a transaction is only a client 
		 * sending INFORM messages then there is no lease information exchanged so 
		 * this is helpful to know who sent the messages. 
		 * 
		 * Getting an address in this field does require that the client sources at 
		 * least one DHCP message using a non-broadcast address.
		 */
		fields.add(Schema.Field.of("client_addr", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* server_addr: IP address of the server involved in actually handing out the 
		 * lease. There could be other servers replying with OFFER messages which won’t 
		 * be represented here. Getting an address in this field also requires that the 
		 * server handing out the lease also sources packets from a non-broadcast IP address.
		 */
		fields.add(Schema.Field.of("server_addr", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* mac: Client’s hardware address.
		 */
		fields.add(Schema.Field.of("mac", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* host_name: Name given by client in Hostname.
		 */
		fields.add(Schema.Field.of("host_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* client_fqdn: FQDN given by client in Client FQDN
		 */
		fields.add(Schema.Field.of("client_fqdn", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* domain: Domain given by the server
		 */
		fields.add(Schema.Field.of("domain", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* requested_addr: IP address requested by the client.
		 */
		fields.add(Schema.Field.of("requested_addr", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* assigned_addr: IP address assigned by the server.
		 */
		fields.add(Schema.Field.of("assigned_addr", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* lease_time: IP address lease interval.
		 */
		fields.add(Schema.Field.of("lease_time", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* client_message: Message typically accompanied with a DHCP_DECLINE so the 
		 * client can tell the server why it rejected an address.
		 */
		fields.add(Schema.Field.of("client_message", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* server_message: Message typically accompanied with a DHCP_NAK to let the 
		 * client know why it rejected the request.
		 */
		fields.add(Schema.Field.of("server_message", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* msg_types: The DHCP message types seen by this DHCP transaction
		 */
		fields.add(Schema.Field.of("msg_types", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* duration: Duration of the DHCP “session” representing the time from the 
		 * first message to the last.
		 */
		fields.add(Schema.Field.of("duration", Schema.nullableOf(Schema.of(Schema.Type.LONG))));

		/* msg_orig: The address that originated each message from the msg_types field.
		 */
		fields.add(Schema.Field.of("msg_orig", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* client_software: Software reported by the client in the vendor_class option.
		 */
		fields.add(Schema.Field.of("client_software", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* server_software: Software reported by the server in the vendor_class option.
		 */
		fields.add(Schema.Field.of("server_software", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* circuit_id: Added by DHCP relay agents which terminate switched or permanent circuits. 
		 * It encodes an agent-local identifier of the circuit from which a DHCP client-to-server 
		 * packet was received. Typically it should represent a router or switch interface number.
		 */
		fields.add(Schema.Field.of("circuit_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* agent_remote_id: A globally unique identifier added by relay agents to identify the 
		 * remote host end of the circuit.
		 */
		fields.add(Schema.Field.of("agent_remote_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* subscriber_id: The subscriber ID is a value independent of the physical network configuration 
		 * so that a customer’s DHCP configuration can be given to them correctly no matter where they are 
		 * physically connected.
		 */
		fields.add(Schema.Field.of("subscriber_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		Schema schema = Schema.recordOf("dhcp_log", fields);
		return schema;
	
	}
	/*
	 * dnp3 (&log)
	 * 
	 * A Log of very basic DNP3 analysis script that just records 
	 * requests and replies.
	 * 
	 * {
	 * 	"ts":1227729908.705944,
	 * 	"uid":"CQV6tj1w1t4WzQpHoe",
	 * 	"id.orig_h":"127.0.0.1",
	 * 	"id.orig_p":42942,
	 * 	"id.resp_h":"127.0.0.1",
	 * 	"id.resp_p":20000,
	 * 	"fc_request":"READ"
	 * }
	 */
	public static StructuredRecord fromDnp3(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, dnp3());

	}
	
	private static Schema dnp3() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Time of the request.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* fc_request: The name of the function message in the request.
		 */
		fields.add(Schema.Field.of("fc_request", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* fc_reply: The name of the function message in the reply.
		 */
		fields.add(Schema.Field.of("fc_reply", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* iin: The response’s “internal indication number”.
		 */
		fields.add(Schema.Field.of("iin", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		Schema schema = Schema.recordOf("dnp3_log", fields);
		return schema;
	
	}
	
	/*
	 * dns (&log)
	 * 
	 * {
	 * 	"ts":1547188415.857497,
	 * 	"uid":"CAcJw21BbVedgFnYH3",
	 * 	"id.orig_h":"192.168.86.167",
	 * 	"id.orig_p":38339,
	 * 	"id.resp_h":"192.168.86.1",
	 * 	"id.resp_p":53,
	 * 	"proto":"udp",
	 * 	"trans_id":15209,
	 * 	"rtt":0.076967,
	 * 	"query":"dd625ffb4fc54735b281862aa1cd6cd4.us-west1.gcp.cloud.es.io",
	 * 	"qclass":1,
	 * 	"qclass_name":"C_INTERNET",
	 * 	"qtype":1,
	 * 	"qtype_name":"A",
	 * 	"rcode":0,
	 * 	"rcode_name":"NOERROR",
	 * 	"AA":false,
	 * 	"TC":false,
	 * 	"RD":true,
	 * 	"RA":true,
	 * 	"Z":0,
	 * 	"answers":["proxy-production-us-west1.gcp.cloud.es.io","proxy-production-us-west1-v1-009.gcp.cloud.es.io","35.199.178.4"],
	 * 	"TTLs":[119.0,119.0,59.0],
	 * 	"rejected":false
	 * }
	 */
	public static StructuredRecord fromDns(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceInterval(newObject, "rtt");
		
		newObject = replaceConnId(newObject);
		
		newObject = replaceName(newObject, "dns_aa", "AA");
		newObject = replaceName(newObject, "dns_tc", "TC");
		
		newObject = replaceName(newObject, "dns_ra", "RA");
		newObject = replaceName(newObject, "dns_z", "Z");
		
		/*
		 * Rename and transform 'TTLs'
		 */
		newObject = replaceName(newObject, "dns_ttls", "TTLs");
		JsonArray ttls = newObject.remove("dns_ttls").getAsJsonArray();
		
		List<Long> new_ttls = new ArrayList<>();
		
		Iterator<JsonElement> iter = ttls.iterator();
		while (iter.hasNext()) {
			
			long interval = 0L;
			try {
				
				Double ts = iter.next().getAsDouble();
				interval = (long) (ts * 1000);
				
			} catch (Exception e) {
				;
			}
			
			new_ttls.add(interval);
			
		}
		newObject.add("dns_ttls",GSON.toJsonTree(new_ttls).getAsJsonArray());
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, dns());

	}
	
	private static Schema dns() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: The earliest time at which a DNS protocol message over the 
		 * associated connection is observed.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection over which DNS 
		 * messages are being transferred.
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* proto: The transport layer protocol of the connection.
		 */
		fields.add(Schema.Field.of("proto", Schema.of(Schema.Type.STRING)));
		
		/* trans_id: A 16-bit identifier assigned by the program that generated 
		 * the DNS query. Also used in responses to match up replies to outstanding 
		 * queries.
		 */
		fields.add(Schema.Field.of("trans_id", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* rtt: Round trip time for the query and response. This indicates the delay 
		 * between when the request was seen until the answer started.
		 */
		fields.add(Schema.Field.of("rtt", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* query: The domain name that is the subject of the DNS query.
		 */
		fields.add(Schema.Field.of("query", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* qclass: The QCLASS value specifying the class of the query.
		 */
		fields.add(Schema.Field.of("qclass", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* qclass_name: A descriptive name for the class of the query.
		 */
		fields.add(Schema.Field.of("qclass_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* qtype: A QTYPE value specifying the type of the query.
		 */
		fields.add(Schema.Field.of("qtype", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* qtype_name: A descriptive name for the type of the query.
		 */
		fields.add(Schema.Field.of("qtype_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* rcode: The response code value in DNS response messages.
		 */
		fields.add(Schema.Field.of("rcode", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* rcode_name: A descriptive name for the response code value.
		 */
		fields.add(Schema.Field.of("rcode_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* AA: The Authoritative Answer bit for response messages specifies 
		 * that the responding name server is an authority for the domain 
		 * name in the question section.
		 */
		fields.add(Schema.Field.of("dns_aa", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* TC: The Recursion Desired bit in a request message indicates that 
		 * the client wants recursive service for this query.
		 */
		fields.add(Schema.Field.of("dns_tc", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* RA: The Recursion Available bit in a response message indicates that 
		 * the name server supports recursive queries.
		 */
		fields.add(Schema.Field.of("dns_ra", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* Z: A reserved field that is usually zero in queries and responses.
		 */
		fields.add(Schema.Field.of("dns_z", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* answers: The set of resource descriptions in the query answer.
		 */
		fields.add(Schema.Field.of("answers", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* TTLs: The caching intervals of the associated RRs described by the answers field.
		 */
		fields.add(Schema.Field.of("dns_ttls", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.LONG)))));
		
		/* rejected: The DNS query was rejected by the server.
		 */
		fields.add(Schema.Field.of("rejected", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* auth: Authoritative responses for the query.
		 */
		fields.add(Schema.Field.of("auth", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* addl: Additional responses for the query.
		 */
		fields.add(Schema.Field.of("addl", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		Schema schema = Schema.recordOf("dns_log", fields);
		return schema;
	
	}
	
	/*
	 * dpd (&log)
	 * 
	 * Dynamic protocol detection failures.
	 * 
	 * {
	 * 	"ts":1507567500.423033,
	 * 	"uid":"CRrT7S1ccw9H6hzCR",
	 * 	"id.orig_h":"192.168.10.31",
	 * 	"id.orig_p":49285,
	 * 	"id.resp_h":"192.168.10.10",
	 * 	"id.resp_p":445,
	 * 	"proto":"tcp",
	 * 	"analyzer":"DCE_RPC",
	 * 	"failure_reason":"Binpac exception: binpac exception: \u0026enforce violation : DCE_RPC_Header:rpc_vers"
	 * }
	 */
	public static StructuredRecord fromDpd(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, dpd());

	}
	
	private static Schema dpd() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: imestamp for when protocol analysis failed.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: Connection unique ID.
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* proto: The transport layer protocol of the connection.
		 */
		fields.add(Schema.Field.of("proto", Schema.of(Schema.Type.STRING)));
		
		/* analyzer: The analyzer that generated the violation.
		 */
		fields.add(Schema.Field.of("analyzer", Schema.of(Schema.Type.STRING)));
		
		/* failure_reason: The textual reason for the analysis failure.
		 */
		fields.add(Schema.Field.of("failure_reason", Schema.of(Schema.Type.STRING)));
		
		/* packet_segment: A chunk of the payload that most likely resulted in 
		 * the protocol violation.
		 */
		fields.add(Schema.Field.of("packet_segment", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		Schema schema = Schema.recordOf("dpd_log", fields);
		return schema;
	
	}
	
	/*
	 * files (&log)
	 * 
	 * {
	 * 	"ts":1547688796.636812,
	 * 	"fuid":"FMkioa222mEuM2RuQ9",
	 * 	"tx_hosts":["35.199.178.4"],
	 * 	"rx_hosts":["10.178.98.102"],
	 * 	"conn_uids":["C8I0zn3r9EPbfLgta6"],
	 * 	"source":"SSL",
	 * 	"depth":0,
	 * 	"analyzers":["X509","MD5","SHA1"],
	 * 	"mime_type":"application/pkix-cert",
	 * 	"duration":0.0,
	 * 	"local_orig":false,
	 * 	"is_orig":false,
	 * 	"seen_bytes":947,
	 * 	"missing_bytes":0,
	 * 	"overflow_bytes":0,
	 * 	"timedout":false,
	 * 	"md5":"79e4a9840d7d3a96d7c04fe2434c892e",
	 * 	"sha1":"a8985d3a65e5e5c4b2d7d66d40c6dd2fb19c5436"
	 * }
	 */
	public static StructuredRecord fromFiles(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceInterval(newObject, "duration");
		
		newObject = replaceName(newObject, "source_ips", "tx_hosts");
		newObject = replaceName(newObject, "destination_ips", "rx_hosts");
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, files());
		
	}
	/*
	 * IMPORTANT: Despite the example above (from Filebeat),
	 * the current Zeek documentation (v3.1.2) does not specify
	 * a connection id
	 */
	private static Schema files() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: The time when the file was first seen.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* fuid: An identifier associated with a single file. 
		 */
		fields.add(Schema.Field.of("fuid", Schema.of(Schema.Type.STRING)));
		
		/* tx_hosts: If this file was transferred over a network connection 
		 * this should show the host or hosts that the data sourced from.
		 */
		fields.add(Schema.Field.of("source_ips", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* rx_hosts: If this file was transferred over a network connection 
		 * this should show the host or hosts that the data traveled to.
		 */
		fields.add(Schema.Field.of("destination_ips", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* conn_uids: Connection UIDs over which the file was transferred.
		 */
		fields.add(Schema.Field.of("conn_uids", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* source: An identification of the source of the file data. 
		 * E.g. it may be a network protocol over which it was transferred, 
		 * or a local file path which was read, or some other input source.
		 */
		fields.add(Schema.Field.of("source", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* depth: A value to represent the depth of this file in relation to 
		 * its source. In SMTP, it is the depth of the MIME attachment on the 
		 * message. 
		 * 
		 * In HTTP, it is the depth of the request within the TCP connection.
		 */
		fields.add(Schema.Field.of("depth", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* analyzers: A set of analysis types done during the file analysis.
		 */
		fields.add(Schema.Field.of("analyzers", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* mime_type: A mime type provided by the strongest file magic signature 
		 * match against the bof_buffer field of fa_file, or in the cases where 
		 * no buffering of the beginning of file occurs, an initial guess of the 
		 * mime type based on the first data seen.
		 */
		fields.add(Schema.Field.of("mime_type", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* filename: A filename for the file if one is available from the source 
		 * for the file. These will frequently come from “Content-Disposition” 
		 * headers in network protocols.
		 */
		fields.add(Schema.Field.of("filename", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* duration: The duration the file was analyzed for.
		 */
		fields.add(Schema.Field.of("duration", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* local_orig: If the source of this file is a network connection, this field 
		 * indicates if the data originated from the local network or not as determined 
		 * by the configured
		 */
		fields.add(Schema.Field.of("local_orig", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* is_orig: If the source of this file is a network connection, this field indicates 
		 * if the file is being sent by the originator of the connection or the responder.
		 */
		fields.add(Schema.Field.of("is_orig", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* seen_bytes: Number of bytes provided to the file analysis engine for the file.
		 */
		fields.add(Schema.Field.of("seen_bytes", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* total_bytes: Total number of bytes that are supposed to comprise the full file.
		 */
		fields.add(Schema.Field.of("total_bytes", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* missing_bytes: The number of bytes in the file stream that were completely missed 
		 * during the process of analysis e.g. due to dropped packets.
		 */
		fields.add(Schema.Field.of("missing_bytes", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* overflow_bytes: The number of bytes in the file stream that were not delivered 
		 * to stream file analyzers. This could be overlapping bytes or bytes that couldn’t 
		 * be reassembled.
		 */
		fields.add(Schema.Field.of("overflow_bytes", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* timedout: Whether the file analysis timed out at least once for the file.
		 */
		fields.add(Schema.Field.of("timedout", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* parent_fuid: Identifier associated with a container file from which this one 
		 * was extracted as part of the file analysis.
		 */
		fields.add(Schema.Field.of("parent_fuid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* md5: An MD5 digest of the file contents.
		 */
		fields.add(Schema.Field.of("md5", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* sha1: A SHA1 digest of the file contents.
		 */
		fields.add(Schema.Field.of("sha1", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* sha256: A SHA256 digest of the file contents.
		 */
		fields.add(Schema.Field.of("sha256", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* extracted: Local filename of extracted file.
		 */
		fields.add(Schema.Field.of("extracted", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* extracted_cutoff: Set to true if the file being extracted was cut off so the
		 * whole file was not logged.
		 */
		fields.add(Schema.Field.of("extracted_cutoff", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* extracted_size: The number of bytes extracted to disk.
		 */
		fields.add(Schema.Field.of("extracted_size", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* entropy: The information density of the contents of the file, expressed 
		 * as a number of bits per character.
		 */
		fields.add(Schema.Field.of("entropy", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
		
		Schema schema = Schema.recordOf("files_log", fields);
		return schema;
	
	}
	/*
	 * ftp (& log)
	 * 
	 * FTP activity
	 * 
	 * {
	 * 	"ts":1187379104.955342,
	 * 	"uid":"CpQoCn3o28tke89zv9",
	 * 	"id.orig_h":"192.168.1.182",
	 * 	"id.orig_p":62014,
	 * 	"id.resp_h":"192.168.1.231",
	 * 	"id.resp_p":21,
	 * 	"user":"ftp",
	 * 	"password":"ftp",
	 * 	"command":"EPSV",
	 * 	"reply_code":229,
	 * 	"reply_msg":"Entering Extended Passive Mode (|||37100|)",
	 * 	"data_channel.passive":true,
	 * 	"data_channel.orig_h":"192.168.1.182",
	 * 	"data_channel.resp_h":"192.168.1.231",
	 * 	"data_channel.resp_p":37100
	 * }
	 */
	public static StructuredRecord fromFtp(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");		
		newObject = replaceConnId(newObject);
		
		newObject = replaceName(newObject, "data_channel_passive", "data_channel.passive");
		newObject = replaceName(newObject, "data_channel_source_ip", "data_channel.orig_h");
		
		newObject = replaceName(newObject, "data_channel_destination_ip", "data_channel.resp_h");
		newObject = replaceName(newObject, "data_channel_destination_port", "data_channel.resp_p");
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, ftp());

	}
	
	private static Schema ftp() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Time when the command was sent.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* user: User name for the current FTP session. 
		 */
		fields.add(Schema.Field.of("user", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* password: Password for the current FTP session if captured.
		 */
		fields.add(Schema.Field.of("password", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* command: Command given by the client. 
		 */
		fields.add(Schema.Field.of("command", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* arg: Argument for the command if one is given.
		 */
		fields.add(Schema.Field.of("arg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* mime_type: Sniffed mime type of file.
		 */
		fields.add(Schema.Field.of("mime_type", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* file_size: Size of the file if the command indicates a file transfer.
		 */
		fields.add(Schema.Field.of("file_size", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* reply_code: Reply code from the server in response to the command.
		 */
		fields.add(Schema.Field.of("reply_code", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* reply_msg: Reply message from the server in response to the command.
		 */
		fields.add(Schema.Field.of("reply_msg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/*** data_channel ***/
		
		/* data_channel.passive: Whether PASV mode is toggled for control channel.
		 */
		fields.add(Schema.Field.of("data_channel_passive", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* data_channel.orig_h: The host that will be initiating the data connection.
		 */
		fields.add(Schema.Field.of("data_channel_source_ip", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* data_channel.resp_h: The host that will be accepting the data connection.
		 */
		fields.add(Schema.Field.of("data_channel_destination_ip", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* data_channel.resp_p: The port at which the acceptor is listening for the data connection.
		 */
		fields.add(Schema.Field.of("data_channel_destination_port", Schema.nullableOf(Schema.of(Schema.Type.INT))));

		/* fuid: File unique ID.
		 */
		fields.add(Schema.Field.of("fuid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		Schema schema = Schema.recordOf("ftp_log", fields);
		return schema;
	
	}
	/*
	 * http (&log)
	 * 
	 * The logging model is to log request/response pairs and all 
	 * relevant metadata together in a single record.
	 * 
	 * {
	 * 	"ts":1547687130.172944,
	 * 	"uid":"CCNp8v1SNzY7v9d1Ih",
	 * 	"id.orig_h":"10.178.98.102",
	 * 	"id.orig_p":62995,
	 * 	"id.resp_h":"17.253.5.203",
	 * 	"id.resp_p":80,
	 * 	"trans_depth":1,
	 * 	"method":"GET",
	 * 	"host":"ocsp.apple.com",
	 * 	"uri":"/ocsp04-aaica02/ME4wTKADAgEAMEUwQzBBMAkGBSsOAwIaBQAEFNqvF+Za6oA4ceFRLsAWwEInjUhJBBQx6napI3Sl39T97qDBpp7GEQ4R7AIIUP1IOZZ86ns=",
	 * 	"version":"1.1",
	 * 	"user_agent":"com.apple.trustd/2.0",
	 * 	"request_body_len":0,
	 * 	"response_body_len":3735,
	 * 	"status_code":200,
	 * 	"status_msg":"OK",
	 * 	"tags":[],
	 * 	"resp_fuids":["F5zuip1tSwASjNAHy7"],
	 * 	"resp_mime_types":["application/ocsp-response"]
	 * }
	 */
	public static StructuredRecord fromHttp(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");		
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, http());
		
	}
	
	private static Schema http() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the request happened.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* trans_depth: Represents the pipelined depth into the connection 
		 * of this request/response transaction.
		 */
		fields.add(Schema.Field.of("trans_depth", Schema.of(Schema.Type.INT)));
		
		/* method: Verb used in the HTTP request (GET, POST, HEAD, etc.).
		 */
		fields.add(Schema.Field.of("method", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* host: Value of the HOST header.
		 */
		fields.add(Schema.Field.of("host", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* uri: URI used in the request.
		 */
		fields.add(Schema.Field.of("uri", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* referrer: Value of the “referer” header. The comment is deliberately misspelled 
		 * like the standard declares, but the name used here is “referrer” spelled correctly.
		 */
		fields.add(Schema.Field.of("referrer", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* version: Value of the version portion of the request.
		 */
		fields.add(Schema.Field.of("version", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* user_agent: Value of the User-Agent header from the client.
		 */
		fields.add(Schema.Field.of("user_agent", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* origin: Value of the Origin header from the client.
		 */
		fields.add(Schema.Field.of("origin", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* request_body_len: Actual uncompressed content size of the data transferred from the client.
		 */
		fields.add(Schema.Field.of("request_body_len", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* response_body_len: Actual uncompressed content size of the data transferred from the server.
		 */
		fields.add(Schema.Field.of("response_body_len", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* status_code: Status code returned by the server.
		 */
		fields.add(Schema.Field.of("status_code", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* status_msg: Status message returned by the server.
		 */
		fields.add(Schema.Field.of("status_msg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* info_code: Last seen 1xx informational reply code returned by the server.
		 */
		fields.add(Schema.Field.of("info_code", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* info_msg: Last seen 1xx informational reply message returned by the server.
		 */
		fields.add(Schema.Field.of("info_msg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* tags: A set of indicators of various attributes discovered and related to a 
		 * particular request/response pair.
		 */
		fields.add(Schema.Field.of("tags", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* username: Username if basic-auth is performed for the request.
		 */
		fields.add(Schema.Field.of("username", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* password: Password if basic-auth is performed for the request.
		 */
		fields.add(Schema.Field.of("password", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* proxied: All of the headers that may indicate if the request was proxied.
		 */
		fields.add(Schema.Field.of("proxied", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* orig_fuids: An ordered vector of file unique IDs. 
		 * Limited to HTTP::max_files_orig entries.
		 */
		fields.add(Schema.Field.of("orig_fuids", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* orig_filenames: An ordered vector of filenames from the client. 
		 * Limited to HTTP::max_files_orig entries.
		 */
		fields.add(Schema.Field.of("orig_filenames", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* orig_mime_types: An ordered vector of mime types. 
		 * Limited to HTTP::max_files_orig entries.
		 */
		fields.add(Schema.Field.of("orig_mime_types", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* resp_fuids: An ordered vector of file unique IDs. 
		 * Limited to HTTP::max_files_resp entries.
		 */
		fields.add(Schema.Field.of("resp_fuids", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* resp_filenames: An ordered vector of filenames from the server. 
		 * Limited to HTTP::max_files_resp entries.
		 */
		fields.add(Schema.Field.of("resp_filenames", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* resp_mime_types: An ordered vector of mime types. 
		 * Limited to HTTP::max_files_resp entries.
		 */
		fields.add(Schema.Field.of("resp_mime_types", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* client_header_names: The vector of HTTP header names sent by the client. 
		 * No header values are included here, just the header names.
		 */
		fields.add(Schema.Field.of("client_header_names", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* server_header_names: The vector of HTTP header names sent by the server. 
		 * No header values are included here, just the header names.
		 */
		fields.add(Schema.Field.of("server_header_names", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* cookie_vars: Variable names extracted from all cookies.
		 */
		fields.add(Schema.Field.of("cookie_vars", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* uri_vars: Variable names from the URI.
		 */
		fields.add(Schema.Field.of("uri_vars", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		Schema schema = Schema.recordOf("http_log", fields);
		return schema;	
	
	}
	/*
	 * intel (&log)
	 * 
	 * {
	 * 	"ts":1573030980.989353,
	 * 	"uid":"Ctefoj1tgOPt4D0EK2",
	 * 	"id.orig_h":"192.168.1.1",
	 * 	"id.orig_p":37598,
	 * 	"id.resp_h":"198.41.0.4",
	 * 	"id.resp_p":53,
	 * 	"seen.indicator":"198.41.0.4",
	 * 	"seen.indicator_type":"Intel::ADDR",
	 * 	"seen.where":"Conn::IN_RESP",
	 * 	"seen.node":"worker-1-2",
	 * 	"matched":["Intel::ADDR"],
	 * 	"sources":["ETPRO Rep: AbusedTLD Score: 127"]
	 * }
	 */
	public static StructuredRecord fromIntel(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);

		newObject = replaceName(newObject, "seen_indicator", "seen.indicator");
		newObject = replaceName(newObject, "seen_indicator_type", "seen.indicator_type");

		newObject = replaceName(newObject, "seen_host", "seen.host");
		newObject = replaceName(newObject, "seen_where", "seen.where");
		
		newObject = replaceName(newObject, "seen_node", "seen.node");
		
		newObject = replaceName(newObject, "cif_tags", "cif.tags");
		newObject = replaceName(newObject, "cif_confidence", "cif.confidence");
		
		newObject = replaceName(newObject, "cif_source", "cif.source");
		newObject = replaceName(newObject, "cif_description", "cif.description");
		
		newObject = replaceName(newObject, "cif_firstseen", "cif.firstseen");
		newObject = replaceName(newObject, "cif_lastseen", "cif.lastseen");
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, intel());
		
	}
	
	private static Schema intel() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp when the data was discovered.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: If a connection was associated with this intelligence hit, 
		 * this is the uid for the connection
		 */
		fields.add(Schema.Field.of("uid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* id 
		 */
		fields.addAll(conn_id_nullable());

		/*** seen: Where the data was seen. ***/
		
		/* seen.indicator: The string if the data is about a string.
		 */
		fields.add(Schema.Field.of("seen_indicator", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* seen.indicator_type: The type of data that the indicator represents.
		 */
		fields.add(Schema.Field.of("seen_indicator_type", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* seen.host: If the indicator type was Intel::ADDR, then this field 
		 * will be present.
		 */
		fields.add(Schema.Field.of("seen_host", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* seen.where: Where the data was discovered.
		 */
		fields.add(Schema.Field.of("seen_where", Schema.of(Schema.Type.STRING)));
		
		/* seen.node: The name of the node where the match was discovered.
		 */
		fields.add(Schema.Field.of("seen_node", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* matched: Which indicator types matched.
		 */
		fields.add(Schema.Field.of("matched", Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		
		/* sources: Sources which supplied data that resulted in this match.
		 */
		fields.add(Schema.Field.of("sources", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* fuid: If a file was associated with this intelligence hit, this is the 
		 * uid for the file.
		 */
		fields.add(Schema.Field.of("fuid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* file_mime_type: A mime type if the intelligence hit is related to a file. 
		 * If the $f field is provided this will be automatically filled out.
		 */
		fields.add(Schema.Field.of("file_mime_type", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* file_desc: Frequently files can be “described” to give a bit more context. 
		 * If the $f field is provided this field will be automatically filled out.
		 */
		fields.add(Schema.Field.of("file_desc", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/*** cif ***/
		
		/* cif.tags: CIF tags observations, examples for tags are botnet or exploit.
		 */
		fields.add(Schema.Field.of("cif_tags", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* cif.confidence: In CIF Confidence details the degree of certainty of a given observation.
		 */
		fields.add(Schema.Field.of("cif_confidence", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
		
		/* cif.source: Source given in CIF.
		 */
		fields.add(Schema.Field.of("cif_source", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* cif.description: Description given in CIF.
		 */
		fields.add(Schema.Field.of("cif_description", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* cif.firstseen: First time the source observed the behavior.
		 */
		fields.add(Schema.Field.of("cif_firstseen", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* cif.lastseen: Last time the source observed the behavior.
		 */
		fields.add(Schema.Field.of("cif_lastseen", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		Schema schema = Schema.recordOf("intel_log", fields);
		return schema;	
	
	}
	
	/*
	 * irc (&log)
	 * 
	 * The logging model is to log IRC commands along with the associated response 
	 * and some additional metadata about the connection if it’s available.
	 * 
	 * {
	 * 	"ts":1387554250.647295,
	 * 	"uid":"CNJBX5FQdL62VUUP1",
	 * 	"id.orig_h":"10.180.156.249",
	 * 	"id.orig_p":45921,
	 * 	"id.resp_h":"38.229.70.20",
	 * 	"id.resp_p":8000,
	 * 	"command":"USER",
	 * 	"value":"xxxxx",
	 * 	"addl":"+iw xxxxx XxxxxxXxxx "
	 * }
	 */
	public static StructuredRecord fromIrc(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");		
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		return StructuredRecordStringConverter.fromJsonString(json, irc());

	}
	
	private static Schema irc() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the command was seen.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());

		/* nick: Nickname given for the connection.
		 */
		fields.add(Schema.Field.of("nick", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* user: Username given for the connection.
		 */
		fields.add(Schema.Field.of("user", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* command: Command given by the client. 
		 */
		fields.add(Schema.Field.of("command", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* value: Value for the command given by the client.
		 */
		fields.add(Schema.Field.of("value", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* addl: Any additional data for the command.
		 */
		fields.add(Schema.Field.of("addl", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* dcc_file_name: DCC filename requested.
		 */
		fields.add(Schema.Field.of("dcc_file_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* dcc_file_size: Size of the DCC transfer as indicated by the sender.
		 */
		fields.add(Schema.Field.of("dcc_file_size", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* dcc_mime_type: Sniffed mime type of the file.
		 */
		fields.add(Schema.Field.of("dcc_mime_type", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* fuid: File unique ID.
		 */
		fields.add(Schema.Field.of("fuid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		Schema schema = Schema.recordOf("irc_log", fields);
		return schema;	
	
	}
	
	/*
	 * kerberos (&log)
	 * 
	 * {
	 * 	"ts":1507565599.590346,
	 * 	"uid":"C56Flhb4WQBNkfMOl",
	 * 	"id.orig_h":"192.168.10.31",
	 * 	"id.orig_p":49242,
	 * 	"id.resp_h":"192.168.10.10",
	 * 	"id.resp_p":88,
	 * 	"request_type":"TGS",
	 * 	"client":"RonHD/CONTOSO.LOCAL",
	 * 	"service":"HOST/admin-pc",
	 * 	"success":true,
	 * 	"till":2136422885.0,
	 * 	"cipher":"aes256-cts-hmac-sha1-96",
	 * 	"forwardable":true,
	 * 	"renewable":true
	 * }
	 */
	public static StructuredRecord fromKerberos(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceTime(newObject, "from");
		newObject = replaceTime(newObject, "till");
		
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		return StructuredRecordStringConverter.fromJsonString(json, kerberos());

	}
	
	private static Schema kerberos() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the event happened.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());

		/* request_type: Request type - Authentication Service (“AS”) or 
		 * Ticket Granting Service (“TGS”)
		 */
		fields.add(Schema.Field.of("request_type", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* client: Client.
		 */
		fields.add(Schema.Field.of("client", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* service: Service. 
		 */
		fields.add(Schema.Field.of("service", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* success: Request result. 
		 */
		fields.add(Schema.Field.of("success", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* error_msg: Error message. 
		 */
		fields.add(Schema.Field.of("error_msg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* from: Ticket valid from. 
		 */
		fields.add(Schema.Field.of("from", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* till: Ticket valid till.
		 */
		fields.add(Schema.Field.of("till", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* cipher: Ticket encryption type.
		 */
		fields.add(Schema.Field.of("cipher", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* forwardable: Forwardable ticket requested. 
		 */
		fields.add(Schema.Field.of("forwardable", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* renewable: Renewable ticket requested. 
		 */
		fields.add(Schema.Field.of("renewable", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* client_cert_subject: Subject of client certificate, if any.
		 */
		fields.add(Schema.Field.of("client_cert_subject", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* client_cert_fuid: File unique ID of client cert, if any.
		 */
		fields.add(Schema.Field.of("client_cert_fuid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* server_cert_subject: Subject of server certificate, if any.
		 */
		fields.add(Schema.Field.of("server_cert_subject", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* server_cert_fuid: File unique ID of server cert, if any.
		 */
		fields.add(Schema.Field.of("server_cert_fuid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* auth_ticket: Hash of ticket used to authorize request/transaction.
		 */
		fields.add(Schema.Field.of("auth_ticket", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* new_ticket: Hash of ticket returned by the KDC.
		 */
		fields.add(Schema.Field.of("new_ticket", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		Schema schema = Schema.recordOf("kerberos_log", fields);
		return schema;	
	
	}
	
	/*
	 * modbus (&log)
	 * 
	 * {
	 * 	"ts":1352718265.222457,
	 * 	"uid":"CpIIXl4DFGswmjH2bl",
	 * 	"id.orig_h":"192.168.1.10",
	 * 	"id.orig_p":64342,
	 * 	"id.resp_h":"192.168.1.164",
	 * 	"id.resp_p":502,
	 * 	"func":"READ_COILS"
	 * }
	 */
	public static StructuredRecord fromModbus(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");		
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		return StructuredRecordStringConverter.fromJsonString(json, modbus());

	}
	
	private static Schema modbus() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the request happened.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* func: The name of the function message that was sent.
		 */
		fields.add(Schema.Field.of("func", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* exception: The exception if the response was a failure.
		 */
		fields.add(Schema.Field.of("exception", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		Schema schema = Schema.recordOf("modbus_log", fields);
		return schema;
	
	}
	
	/*
	 * mysql (&log)
	 */
	public static StructuredRecord fromMysql(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		return StructuredRecordStringConverter.fromJsonString(json, mysql());
	}
	
	private static Schema mysql() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the event happened.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* cmd: The command that was issued.
		 */
		fields.add(Schema.Field.of("cmd", Schema.of(Schema.Type.STRING)));

		/* arg: The argument issued to the command.
		 */
		fields.add(Schema.Field.of("arg", Schema.of(Schema.Type.STRING)));
		
		/* success: Did the server tell us that the command succeeded?
		 */
		fields.add(Schema.Field.of("success", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* rows: The number of affected rows, if any.
		 */
		fields.add(Schema.Field.of("rows", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* response: Server message, if any.
		 */
		fields.add(Schema.Field.of("response", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		Schema schema = Schema.recordOf("mysql_log", fields);
		return schema;
	
	}
	/*
	 * notice (&log)
	 * 
	 * {
	 * 	"ts":1320435875.879278,
	 * 	"note":"SSH::Password_Guessing",
	 * 	"msg":"172.16.238.1 appears to be guessing SSH passwords (seen in 30 connections).",
	 * 	"sub":"Sampled servers:  172.16.238.136, 172.16.238.136, 172.16.238.136, 172.16.238.136, 172.16.238.136",
	 * 	"src":"172.16.238.1",
	 * 	"peer_descr":"bro",
	 * 	"actions":["Notice::ACTION_LOG"],
	 * 	"suppress_for":3600.0,
	 * 	"dropped":false
	 * }
	 */
	public static StructuredRecord fromNotice(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceInterval(newObject, "suppress_for");
		
		newObject = replaceConnId(newObject);

		newObject = replaceName(newObject, "source_ip", "src");
		newObject = replaceName(newObject, "destination_ip", "dst");

		newObject = replaceName(newObject, "source_port", "p");
		
		newObject = replaceName(newObject, "country_code", "remote_location.country_code");
		newObject = replaceName(newObject, "region", "remote_location.region");
		newObject = replaceName(newObject, "city", "remote_location.city");

		newObject = replaceName(newObject, "latitude", "remote_location.latitude");
		newObject = replaceName(newObject, "longitude", "remote_location.longitude");
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, notice());
		
	}
	
	private static Schema notice() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: An absolute time indicating when the notice occurred, 
		 * defaults to the current network time.
		 */
		fields.add(Schema.Field.of("ts", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* uid: A connection UID which uniquely identifies the endpoints 
		 * concerned with the notice. 
		 */
		fields.add(Schema.Field.of("uid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* id
		 */
		fields.addAll(conn_id_nullable());
		
		/* fuid: A file unique ID if this notice is related to a file. If the f field 
		 * is provided, this will be automatically filled out.
		 */
		fields.add(Schema.Field.of("fuid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* file_mime_type: A mime type if the notice is related to a file. If the f field 
		 * is provided, this will be automatically filled out.
		 */
		fields.add(Schema.Field.of("file_mime_type", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* file_desc: Frequently files can be “described” to give a bit more context. 
		 * This field will typically be automatically filled out from an fa_file record. 
		 * 
		 * For example, if a notice was related to a file over HTTP, the URL of the request 
		 * would be shown.
		 */
		fields.add(Schema.Field.of("file_desc", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* proto: The transport protocol. Filled automatically when either conn, 
		 * iconn or p is specified.
		 */
		fields.add(Schema.Field.of("proto", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* note: The Notice::Type of the notice.
		 */
		fields.add(Schema.Field.of("note", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* msg: The Notice::Type of the notice.
		 */
		fields.add(Schema.Field.of("msg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* sub: The human readable sub-message.
		 */
		fields.add(Schema.Field.of("sub", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* src: Source address, if we don’t have a conn_id.
		 */
		fields.add(Schema.Field.of("source_ip", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* dst: Destination address.
		 */
		fields.add(Schema.Field.of("destination_ip", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* p: Associated port, if we don’t have a conn_id.
		 * 
		 * This field is INTERPRETED as source_port as Zeek's documentation
		 * does not clarify on this.
		 */
		fields.add(Schema.Field.of("source_port", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* n: Associated count, or perhaps a status code.
		 */
		fields.add(Schema.Field.of("n", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* peer_descr: Textual description for the peer that raised this notice, 
		 * including name, host address and port.
		 */
		fields.add(Schema.Field.of("peer_descr", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* actions: The actions which have been applied to this notice.
		 */
		fields.add(Schema.Field.of("actions", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* suppress_for: This field indicates the length of time that this unique notice 
		 * should be suppressed.
		 */
		fields.add(Schema.Field.of("suppress_for", Schema.nullableOf(Schema.of(Schema.Type.LONG))));

		/*** remote_location: Add geographic data related to the “remote” host of the connection. ***/
		
		/* remote_location.country_code: The country code.
		 */
		fields.add(Schema.Field.of("country_code", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* remote_location.region: The The region.
		 */
		fields.add(Schema.Field.of("region", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* remote_location.city: The city.
		 */
		fields.add(Schema.Field.of("city", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* remote_location.latitude: The latitude.
		 */
		fields.add(Schema.Field.of("latitude", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
		
		/* remote_location.longitude: longitude.
		 */
		fields.add(Schema.Field.of("longitude", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
		
		/* dropped: Indicate if the $src IP address was dropped and denied network access.
		 */
		fields.add(Schema.Field.of("dropped", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		Schema schema = Schema.recordOf("notice_log", fields);
		return schema;
	
	}
	/*
	 * ntlm (&log)
	 * 
	 * NT LAN Manager (NTLM)
	 * 
	 * {
	 * 	"ts":1508959117.814467,
	 * 	"uid":"CHphiNUKDC20fsy09",
	 * 	"id.orig_h":"192.168.10.50",
	 * 	"id.orig_p":46785,
	 * 	"id.resp_h":"192.168.10.31",
	 * 	"id.resp_p":445,
	 * 	"username":"JeffV",
	 * 	"hostname":"ybaARon55QykXrgu",
	 * 	"domainname":"contoso.local",
	 * 	"server_nb_computer_name":"VICTIM-PC",
	 * 	"server_dns_computer_name":"Victim-PC.contoso.local",
	 * 	"server_tree_name":"contoso.local"
	 * }
	 */
	public static StructuredRecord fromNtlm(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");		
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, ntlm());
		
	}
	
	private static Schema ntlm() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the event happened.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());

		/* username: Username given by the client.
		 */
		fields.add(Schema.Field.of("username", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* hostname: Hostname given by the client.
		 */
		fields.add(Schema.Field.of("hostname", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* domainname: Domainname given by the client.
		 */
		fields.add(Schema.Field.of("domainname", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* server_nb_computer_name: NetBIOS name given by the server in a CHALLENGE.
		 */
		fields.add(Schema.Field.of("server_nb_computer_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* server_dns_computer_name: DNS name given by the server in a CHALLENGE.
		 */
		fields.add(Schema.Field.of("server_dns_computer_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* server_tree_name: Tree name given by the server in a CHALLENGE.
		 */
		fields.add(Schema.Field.of("server_tree_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* success: Indicate whether or not the authentication was successful.
		 */
		fields.add(Schema.Field.of("success", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		Schema schema = Schema.recordOf("ntlm_log", fields);
		return schema;
	
	}	
	/*
	 * ocsp (&log)
	 * 
	 * Online Certificate Status Protocol (OCSP). Only created if policy script is loaded.
	 * 
	 */
	public static StructuredRecord fromOcsp(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceTime(newObject, "revoketime");

		newObject = replaceTime(newObject, "thisUpdate");
		newObject = replaceTime(newObject, "nextUpdate");
		
		newObject = replaceName(newObject, "hash_algorithm", "hashAlgorithm");
		newObject = replaceName(newObject, "issuer_name_hash", "issuerNameHash");
		
		newObject = replaceName(newObject, "issuer_key_hash", "issuerKeyHash");
		newObject = replaceName(newObject, "serial_number", "serialNumber");
		
		newObject = replaceName(newObject, "cert_status", "certStatus");
		newObject = replaceName(newObject, "revoke_time", "revoketime");

		
		newObject = replaceName(newObject, "revoke_reason", "revokereason");
		newObject = replaceName(newObject, "update_this", "thisUpdate");
		
		newObject = replaceName(newObject, "update_next", "nextUpdate");
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, ocsp());
	}
	
	private static Schema ocsp() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Time when the OCSP reply was encountered.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* id: File id of the OCSP reply.
		 */
		fields.add(Schema.Field.of("id", Schema.of(Schema.Type.STRING)));
		
		/* hashAlgorithm: Hash algorithm used to generate issuerNameHash and issuerKeyHash.
		 */
		fields.add(Schema.Field.of("hash_algorithm", Schema.of(Schema.Type.STRING)));
		
		/* issuerNameHash: Hash of the issuer’s distingueshed name.
		 */
		fields.add(Schema.Field.of("issuer_name_hash", Schema.of(Schema.Type.STRING)));
		
		/* issuerKeyHash: Hash of the issuer’s public key.
		 */
		fields.add(Schema.Field.of("issuer_key_hash", Schema.of(Schema.Type.STRING)));
		
		/* serialNumber: Serial number of the affected certificate.
		 */
		fields.add(Schema.Field.of("serial_number", Schema.of(Schema.Type.STRING)));
		
		/* certStatus: Status of the affected certificate.
		 */
		fields.add(Schema.Field.of("cert_status", Schema.of(Schema.Type.STRING)));
		
		/* revoketime: Time at which the certificate was revoked.
		 */
		fields.add(Schema.Field.of("revoke_time", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* revokereason: Reason for which the certificate was revoked.
		 */
		fields.add(Schema.Field.of("revoke_reason", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* thisUpdate: The time at which the status being shown is known 
		 * to have been correct.
		 */
		fields.add(Schema.Field.of("update_this", Schema.of(Schema.Type.LONG)));
		
		/* nextUpdate: The latest time at which new information about the 
		 * status of the certificate will be available.
		 */
		fields.add(Schema.Field.of("update_next", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		Schema schema = Schema.recordOf("ocsp_log", fields);
		return schema;
	
	}
	
	/*
	 * pe (&log)
	 * 
	 * Portable Executable (PE)
	 */
	public static StructuredRecord fromPe(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceTime(newObject, "compile_ts");
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, pe());

	}
	
	private static Schema pe() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Current timestamp.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* id: File id of this portable executable file.
		 */
		fields.add(Schema.Field.of("id", Schema.of(Schema.Type.STRING)));
		
		/* machine: The target machine that the file was compiled for.
		 */
		fields.add(Schema.Field.of("machine", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* compile_ts: The time that the file was created at.
		 */
		fields.add(Schema.Field.of("compile_ts", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* os: The required operating system.
		 */
		fields.add(Schema.Field.of("os", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* subsystem: The subsystem that is required to run this file.
		 */
		fields.add(Schema.Field.of("subsystem", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* is_exe: Is the file an executable, or just an object file?
		 */
		fields.add(Schema.Field.of("is_exe", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* is_64bit: Is the file a 64-bit executable?
		 */
		fields.add(Schema.Field.of("is_64bit", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* uses_aslr: Does the file support Address Space Layout Randomization?
		 */
		fields.add(Schema.Field.of("uses_aslr", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* uses_dep: Does the file support Data Execution Prevention?
		 */
		fields.add(Schema.Field.of("uses_dep", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* uses_code_integrity: Does the file enforce code integrity checks?
		 */
		fields.add(Schema.Field.of("uses_code_integrity", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* uses_seh: Does the file use structured exception handing?
		 */
		fields.add(Schema.Field.of("uses_seh", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* has_import_table: Does the file have an import table?
		 */
		fields.add(Schema.Field.of("has_import_table", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* has_export_table: Does the file have an export table?
		 */
		fields.add(Schema.Field.of("has_export_table", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* has_cert_table: Does the file have an attribute certificate table?
		 */
		fields.add(Schema.Field.of("has_cert_table", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* has_debug_data: Does the file have a debug table?
		 */
		fields.add(Schema.Field.of("has_debug_data", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* section_names: The names of the sections, in order.
		 */
		fields.add(Schema.Field.of("section_names", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		Schema schema = Schema.recordOf("pe_log", fields);
		return schema;
	}
	/*
	 * radius (&log)
	 * 
	 * RADIUS authentication attempts
	 */
	public static StructuredRecord fromRadius(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceInterval(newObject, "ttl");

		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, radius());

	}
	
	private static Schema radius() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the event happened.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* username: The username, if present.
		 */
		fields.add(Schema.Field.of("username", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* mac: MAC address, if present.
		 */
		fields.add(Schema.Field.of("mac", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* framed_addr: The address given to the network access server, if present. 
		 * This is only a hint from the RADIUS server and the network access server 
		 * is not required to honor the address.
		 */
		fields.add(Schema.Field.of("framed_addr", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* tunnel_client: Address (IPv4, IPv6, or FQDN) of the initiator end of the tunnel, 
		 * if present. This is collected from the Tunnel-Client-Endpoint attribute.
		 */
		fields.add(Schema.Field.of("tunnel_client", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* connect_info: Connect info, if present.
		 */
		fields.add(Schema.Field.of("connect_info", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* reply_msg: Reply message from the server challenge. This is frequently shown 
		 * to the user authenticating.
		 */
		fields.add(Schema.Field.of("reply_msg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* result: Successful or failed authentication.
		 */
		fields.add(Schema.Field.of("result", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* ttl: The duration between the first request and either the “Access-Accept” message 
		 * or an error. If the field is empty, it means that either the request or response 
		 * was not seen.
		 */
		fields.add(Schema.Field.of("ttl", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		Schema schema = Schema.recordOf("radius_log", fields);
		return schema;
	
	}	
	
	/*
	 * rdp (&log)
	 * 
	 * RDP Analysis
	 */
	public static StructuredRecord fromRdp(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, rdp());

	}
	
	private static Schema rdp() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the event happened.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* cookie: Cookie value used by the client machine. This is typically a username.
		 */
		fields.add(Schema.Field.of("cookie", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* result: Status result for the connection. It’s a mix between RDP negotation failure 
		 * messages and GCC server create response messages.
		 */
		fields.add(Schema.Field.of("result", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* security_protocol: Security protocol chosen by the server.
		 */
		fields.add(Schema.Field.of("security_protocol", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* client_channels: The channels requested by the client.
		 */
		fields.add(Schema.Field.of("client_channels", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* keyboard_layout: Keyboard layout (language) of the client machine.
		 */
		fields.add(Schema.Field.of("keyboard_layout", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* client_build: RDP client version used by the client machine.
		 */
		fields.add(Schema.Field.of("client_build", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* client_name: Name of the client machine.
		 */
		fields.add(Schema.Field.of("client_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* client_dig_product_id: Product ID of the client machine.
		 */
		fields.add(Schema.Field.of("client_dig_product_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* desktop_width: Desktop width of the client machine.
		 */
		fields.add(Schema.Field.of("desktop_width", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* desktop_height: Desktop height of the client machine.
		 */
		fields.add(Schema.Field.of("desktop_height", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* requested_color_depth: The color depth requested by the client in the 
		 * high_color_depth field.
		 */
		fields.add(Schema.Field.of("requested_color_depth", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* cert_type: If the connection is being encrypted with native RDP encryption, 
		 * this is the type of cert being used.
		 */
		fields.add(Schema.Field.of("cert_type", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* cert_count: The number of certs seen. X.509 can transfer an entire 
		 * certificate chain.
		 */
		fields.add(Schema.Field.of("cert_count", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* cert_permanent: Indicates if the provided certificate or certificate 
		 * chain is permanent or temporary.
		 */
		fields.add(Schema.Field.of("cert_permanent", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* encryption_level: Encryption level of the connection.
		 */
		fields.add(Schema.Field.of("encryption_level", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* encryption_method: Encryption method of the connection.
		 */
		fields.add(Schema.Field.of("encryption_method", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* ssl: Flag the connection if it was seen over SSL.
		 */
		fields.add(Schema.Field.of("ssl", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		Schema schema = Schema.recordOf("rdp_log", fields);
		return schema;
	
	}	
	
	/*
	 * rfb (&log)
	 * 
	 * Remote Framebuffer (RFB)
	 */
	public static StructuredRecord fromRfb(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, rfb());

	}
	
	private static Schema rfb() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the event happened.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* client_major_version: Major version of the client.
		 */
		fields.add(Schema.Field.of("client_major_version", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* client_minor_version: Minor version of the client.
		 */
		fields.add(Schema.Field.of("client_minor_version", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* server_major_version: Major version of the server.
		 */
		fields.add(Schema.Field.of("server_major_version", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* server_minor_version: Minor version of the server.
		 */
		fields.add(Schema.Field.of("server_minor_version", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* authentication_method: Identifier of authentication method used.
		 */
		fields.add(Schema.Field.of("authentication_method", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* auth: Whether or not authentication was successful.
		 */
		fields.add(Schema.Field.of("auth", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* share_flag: Whether the client has an exclusive or a shared session.
		 */
		fields.add(Schema.Field.of("share_flag", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* desktop_name: Name of the screen that is being shared.
		 */
		fields.add(Schema.Field.of("desktop_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* width: Width of the screen that is being shared.
		 */
		fields.add(Schema.Field.of("width", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* height: Height of the screen that is being shared.
		 */
		fields.add(Schema.Field.of("height", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		Schema schema = Schema.recordOf("rfb_log", fields);
		return schema;
	
	}	
	
	/*
	 * sip (&log)
	 * 
	 * Log from SIP analysis. The logging model is to log request/response pairs 
	 * and all relevant metadata together in a single record.
	 * 
	 * {
	 * 	"ts":1361916159.055464,
	 * 	"uid":"CPRLCB4eWHdjP852Bk",
	 * 	"id.orig_h":"172.16.133.19",
	 * 	"id.orig_p":5060,
	 * 	"id.resp_h":"74.63.41.218",
	 * 	"id.resp_p":5060,
	 * 	"trans_depth":0,
	 * 	"method":"REGISTER",
	 * 	"uri":"sip:newyork.voip.ms:5060",
	 * 	"request_from":"\u0022AppNeta\u0022 <sip:116954_Boston6@newyork.voip.ms>",
	 * 	"request_to":"<sip:116954_Boston6@newyork.voip.ms>",
	 * 	"response_from":"\u0022AppNeta\u0022 <sip:116954_Boston6@newyork.voip.ms>",
	 * 	"response_to":"<sip:116954_Boston6@newyork.voip.ms>;tag=as023f66a5",
	 * 	"call_id":"8694cd7e-976e4fc3-d76f6e38@172.16.133.19",
	 * 	"seq":"4127 REGISTER",
	 * 	"request_path":["SIP/2.0/UDP 172.16.133.19:5060"],
	 * 	"response_path":["SIP/2.0/UDP 172.16.133.19:5060"],
	 * 	"user_agent":"PolycomSoundStationIP-SSIP_5000-UA/3.2.4.0267",
	 * 	"status_code":401,
	 * 	"status_msg":"Unauthorized",
	 * 	"request_body_len":0,
	 * 	"response_body_len":0
	 * }
	 */
	public static StructuredRecord fromSip(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");		
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, sip());
		
	}
	
	private static Schema sip() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the event happened.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* trans_depth: Represents the pipelined depth into the connection of this 
		 * request/response transaction. 
		 */
		fields.add(Schema.Field.of("trans_depth", Schema.of(Schema.Type.INT)));
		
		/* method: Verb used in the SIP request (INVITE, REGISTER etc.).
		 */
		fields.add(Schema.Field.of("method", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* uri: URI used in the request.
		 */
		fields.add(Schema.Field.of("uri", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* date: Contents of the Date: header from the client.
		 */
		fields.add(Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* request_from: Contents of the request From: header Note: The tag= value that’s usually 
		 * appended to the sender is stripped off and not logged.
		 */
		fields.add(Schema.Field.of("request_from", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* request_to: Contents of the To: header.
		 */
		fields.add(Schema.Field.of("request_to", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* response_from: Contents of the response From: header Note: The tag= value that’s usually 
		 * appended to the sender is stripped off and not logged.
		 */
		fields.add(Schema.Field.of("response_from", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* response_to: Contents of the response To: header
		 */
		fields.add(Schema.Field.of("response_to", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* reply_to: Contents of the Reply-To: header
		 */
		fields.add(Schema.Field.of("reply_to", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* call_id: Contents of the Call-ID: header from the client
		 */
		fields.add(Schema.Field.of("call_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* seq: Contents of the CSeq: header from the client
		 */
		fields.add(Schema.Field.of("seq", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* subject: Contents of the Subject: header from the client
		 */
		fields.add(Schema.Field.of("subject", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* request_path: The client message transmission path, as extracted from the headers.
		 */
		fields.add(Schema.Field.of("request_path", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* response_path: The server message transmission path, as extracted from the headers.
		 */
		fields.add(Schema.Field.of("response_path", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* user_agent: Contents of the User-Agent: header from the client
		 */
		fields.add(Schema.Field.of("user_agent", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* status_code: Status code returned by the server.
		 */
		fields.add(Schema.Field.of("status_code", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* status_msg: Status message returned by the server.
		 */
		fields.add(Schema.Field.of("status_msg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* warning: Contents of the Warning: header
		 */
		fields.add(Schema.Field.of("warning", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* request_body_len: Contents of the Content-Length: header from the client
		 */
		fields.add(Schema.Field.of("request_body_len", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* response_body_len: Contents of the Content-Length: header from the server
		 */
		fields.add(Schema.Field.of("response_body_len", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* content_type: Contents of the Content-Type: header from the server
		 */
		fields.add(Schema.Field.of("content_type", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		Schema schema = Schema.recordOf("sip_log", fields);
		return schema;
	
	}	
	
	/*
	 * smb_cmd (&log)
	 * 
	 * SMB commands
	 * 
	 * {
	 * 	"ts":1361916332.020006,
	 * 	"uid":"CbT8mpAXseu6Pt4R7",
	 * 	"id.orig_h":"172.16.133.6",
	 * 	"id.orig_p":1728,
	 * 	"id.resp_h":"172.16.128.202",
	 * 	"id.resp_p":445,
	 * 	"command":"NT_CREATE_ANDX",
	 * 	"argument":"\u005cbrowser",
	 * 	"status":"SUCCESS",
	 * 	"rtt":0.091141,
	 * 	"version":"SMB1",
	 * 	"tree":"\u005c\u005cJSRVR20\u005cIPC$",
	 * 	"tree_service":"IPC",
	 * 	"referenced_file.ts":1361916332.020006,
	 * 	"referenced_file.uid":"CbT8mpAXseu6Pt4R7",
	 * 	"referenced_file.id.orig_h":"172.16.133.6",
	 * 	"referenced_file.id.orig_p":1728,
	 * 	"referenced_file.id.resp_h":"172.16.128.202",
	 * 	"referenced_file.id.resp_p":445,
	 * 	"referenced_file.action":"SMB::FILE_OPEN",
	 * 	"referenced_file.name":"\u005cbrowser",
	 * 	"referenced_file.size":0
	 * }
	 */
	public static StructuredRecord fromSmbCmd(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceInterval(newObject, "rtt");
		
		newObject = replaceConnId(newObject);
		
		newObject = replaceName(newObject, "file_ts", "referenced_file.ts");
		newObject = replaceTime(newObject, "file_ts");

		newObject = replaceName(newObject, "file_uid", "referenced_file.uid");
		newObject = replaceName(newObject, "file_source_ip", "referenced_file.id.orig_h");

		newObject = replaceName(newObject, "file_source_port", "referenced_file.id.orig_p");
		newObject = replaceName(newObject, "file_destination_ip", "referenced_file.id.resp_h");

		newObject = replaceName(newObject, "file_destination_port", "referenced_file.id.resp_p");
		newObject = replaceName(newObject, "file_fuid", "referenced_file.fuid");
		
		newObject = replaceName(newObject, "file_action", "referenced_file.action");
		newObject = replaceName(newObject, "file_path", "referenced_file.path");
		
		newObject = replaceName(newObject, "file_name", "referenced_file.name");
		newObject = replaceName(newObject, "file_size", "referenced_file.size");
		
		newObject = replaceName(newObject, "file_prev_name", "referenced_file.prev_name");
		
		newObject = replaceName(newObject, "", "referenced_file.times.modified");
		newObject = replaceTime(newObject, "file_times_modified");

		newObject = replaceName(newObject, "file_times_accessed", "referenced_file.times.accessed");
		newObject = replaceTime(newObject, "file_times_accessed");
		
		newObject = replaceName(newObject, "file_times_created", "referenced_file.times.created");
		newObject = replaceTime(newObject, "file_times_created");

		newObject = replaceName(newObject, "file_times_changed", "referenced_file.times.changed");
		newObject = replaceTime(newObject, "file_times_changed");
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, smb_cmd());
	}
	
	private static Schema smb_cmd() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the event happened.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* command: The command sent by the client. 
		 */
		fields.add(Schema.Field.of("command", Schema.of(Schema.Type.STRING)));
		
		/* sub_command: The subcommand sent by the client, if present.
		 */
		fields.add(Schema.Field.of("sub_command", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* argument: Command argument sent by the client, if any.
		 */
		fields.add(Schema.Field.of("argument", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* status: Server reply to the client’s command.
		 */
		fields.add(Schema.Field.of("status", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* rtt: Round trip time from the request to the response.
		 */
		fields.add(Schema.Field.of("rtt", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* version: Version of SMB for the command.
		 */
		fields.add(Schema.Field.of("version", Schema.of(Schema.Type.STRING)));
		
		/* username: Authenticated username, if available.
		 */
		fields.add(Schema.Field.of("username", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* tree: If this is related to a tree, this is the tree that was used 
		 * for the current command.
		 */
		fields.add(Schema.Field.of("tree", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* tree_service: The type of tree (disk share, printer share, 
		 * named pipe, etc.).
		 */
		fields.add(Schema.Field.of("tree_service", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/*** referenced_file: If the command referenced a file, store it here. ***/
		
		/* referenced_file.ts: Time when the file was first discovered.
		 */
		fields.add(Schema.Field.of("file_ts", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* referenced_file.uid: Unique ID of the connection the file was sent over.
		 */
		fields.add(Schema.Field.of("file_uid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/*** referenced_file.id: ID of the connection the file was sent over. ***/
		
		/* referenced_file.id.orig_h: The originator’s IP address. 
		 */
		fields.add(Schema.Field.of("file_source_ip", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* referenced_file.id.orig_p: The originator’s port number. 
		 */
		fields.add(Schema.Field.of("file_source_port", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* referenced_file.id.resp_h: The responder’s IP address. 
		 */
		fields.add(Schema.Field.of("file_destination_ip", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* referenced_file.id.resp_p: The responder’s port number.
		 */
		fields.add(Schema.Field.of("file_destination_port", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* referenced_file.fuid: Unique ID of the file.
		 */
		fields.add(Schema.Field.of("file_fuid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* referenced_file.action: Action this log record represents.
		 */
		fields.add(Schema.Field.of("file_action", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* referenced_file.path: Path pulled from the tree this file was transferred to or from.
		 */
		fields.add(Schema.Field.of("file_path", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* referenced_file.name: Filename if one was seen.
		 */
		fields.add(Schema.Field.of("file_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* referenced_file.size: Total size of the file.
		 */
		fields.add(Schema.Field.of("file_size", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* referenced_file.prev_name: If the rename action was seen, 
		 * this will be the file’s previous name.
		 */
		fields.add(Schema.Field.of("file_prev_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/*** times: Last time this file was modified. ***/
		
		/* referenced_file.times.modified: The time when data was last written to the file.
		 */
		fields.add(Schema.Field.of("file_times_modified", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* referenced_file.times.accessed: The time when the file was last accessed.
		 */
		fields.add(Schema.Field.of("file_times_accessed", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* referenced_file.times.created: The time the file was created.
		 */
		fields.add(Schema.Field.of("file_times_created", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* referenced_file.times.changed: The time when the file was last modified.
		 */
		fields.add(Schema.Field.of("file_times_changed", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		Schema schema = Schema.recordOf("smb_cmd_log", fields);
		return schema;
	
	}	
	
	/*
	 * smb_files (&log)
	 * 
	 * SMB files
	 * 
	 * {
	 * 	"ts":1507565599.576942,
	 * 	"uid":"C9YAaEzWLL62yWMn5",
	 * 	"id.orig_h":"192.168.10.31",
	 * 	"id.orig_p":49239,
	 * 	"id.resp_h":"192.168.10.30",
	 * 	"id.resp_p":445,
	 * 	"action":"SMB::FILE_OPEN",
	 * 	"path":"\u005c\u005cadmin-pc\u005cADMIN$",
	 * 	"name":"PSEXESVC.exe",
	 * 	"size":0,
	 * 	"times.modified":1507565599.607777,
	 * 	"times.accessed":1507565599.607777,
	 * 	"times.created":1507565599.607777,
	 * 	"times.changed":1507565599.607777
	 * }
	 */
	public static StructuredRecord fromSmbFiles(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");

		newObject = replaceTime(newObject, "times.modified");
		newObject = replaceTime(newObject, "times.accessed");

		newObject = replaceTime(newObject, "times.created");
		newObject = replaceTime(newObject, "times.changed");
		
		newObject = replaceConnId(newObject);
		
		newObject = replaceName(newObject, "times_modified", "times.modified");
		newObject = replaceName(newObject, "times_accessed", "times.accessed");

		newObject = replaceName(newObject, "times_created", "times.created");
		newObject = replaceName(newObject, "times_changed", "times.changed");
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, smb_files());

	}
	
	private static Schema smb_files() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Time when the file was first discovered.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* fuid: Unique ID of the file.
		 */
		fields.add(Schema.Field.of("fuid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* action: Action this log record represents.
		 */
		fields.add(Schema.Field.of("action", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* path: Path pulled from the tree this file was transferred to or from.
		 */
		fields.add(Schema.Field.of("path", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* name: Filename if one was seen.
		 */
		fields.add(Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* size: Total size of the file.
		 */
		fields.add(Schema.Field.of("size", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* prev_name: If the rename action was seen, this will be the file’s previous name.
		 */
		fields.add(Schema.Field.of("prev_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/*** times: Last time this file was modified. ***/
		
		/* times.modified: The time when data was last written to the file.
		 */
		fields.add(Schema.Field.of("times_modified", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* times.accessed: The time when the file was last accessed.
		 */
		fields.add(Schema.Field.of("times_accessed", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* times.created: The time the file was created.
		 */
		fields.add(Schema.Field.of("times_created", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* times.changed: The time when the file was last modified.
		 */
		fields.add(Schema.Field.of("times_changed", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		Schema schema = Schema.recordOf("smb_files_log", fields);
		return schema;
	
	}	
	
	/*
	 * smb_mapping (&log)
	 * 
	 * SMB Trees
	 * 
	 * {
	 * 	"ts":1507565599.576613,
	 * 	"uid":"C9YAaEzWLL62yWMn5",
	 * 	"id.orig_h":"192.168.10.31",
	 * 	"id.orig_p":49239,
	 * 	"id.resp_h":"192.168.10.30",
	 * 	"id.resp_p":445,
	 * 	"path":"\u005c\u005cadmin-pc\u005cADMIN$",
	 * 	"share_type":"DISK"
	 * }
	 */
	public static StructuredRecord fromSmbMapping(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, smb_mapping());

	}
	
	private static Schema smb_mapping() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Time when the tree was mapped.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* path: Name of the tree path.
		 */
		fields.add(Schema.Field.of("path", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* service: The type of resource of the tree (disk share, printer share, 
		 * named pipe, etc.).
		 */
		fields.add(Schema.Field.of("service", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* native_file_system: File system of the tree.
		 */
		fields.add(Schema.Field.of("native_file_system", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* share_type: If this is SMB2, a share type will be included. For SMB1, 
		 * the type of share will be deduced and included as well.
		 */
		fields.add(Schema.Field.of("share_type", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		Schema schema = Schema.recordOf("smb_mapping_log", fields);
		return schema;
	
	}	
	
	/*
	 * smtp (&log)
	 * 
	 * SMTP transactions
	 * 
	 * {
	 * 	"ts":1543877987.381899,
	 * 	"uid":"CWWzPB3RjqhFf528c",
	 * 	"id.orig_h":"192.168.1.10",
	 * 	"id.orig_p":33782,
	 * 	"id.resp_h":"192.168.1.9",
	 * 	"id.resp_p":25,
	 * 	"trans_depth":1,
	 * 	"helo":"EXAMPLE.COM",
	 * 	"last_reply":"220 2.0.0 SMTP server ready",
	 * 	"path":["192.168.1.9"],
	 * 	"tls":true,
	 * 	"fuids":[],
	 * 	"is_webmail":false
	 * }
	 */
	public static StructuredRecord fromSmtp(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, smtp());
	}
	
	private static Schema smtp() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Time when the message was first seen.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* trans_depth: A count to represent the depth of this message transaction 
		 * in a single connection where multiple messages were transferred. 
		 */
		fields.add(Schema.Field.of("trans_depth", Schema.of(Schema.Type.INT)));

		/* helo: Contents of the Helo header.
		 */
		fields.add(Schema.Field.of("helo", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* mailfrom: Email addresses found in the From header.
		 */
		fields.add(Schema.Field.of("mailfrom", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* rcptto: Email addresses found in the Rcpt header.
		 */
		fields.add(Schema.Field.of("rcptto", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));

		/* date: Contents of the Date header.
		 */
		fields.add(Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* from: Contents of the From header.
		 */
		fields.add(Schema.Field.of("from", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* to: Contents of the To header.
		 */
		fields.add(Schema.Field.of("to", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));

		/* cc: Contents of the CC header.
		 */
		fields.add(Schema.Field.of("cc", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));

		/* reply_to: Contents of the ReplyTo header.
		 */
		fields.add(Schema.Field.of("reply_to", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* msg_id: Contents of the MsgID header.
		 */
		fields.add(Schema.Field.of("msg_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* in_reply_to: Contents of the In-Reply-To header.
		 */
		fields.add(Schema.Field.of("in_reply_to", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* subject: Contents of the Subject header.
		 */
		fields.add(Schema.Field.of("subject", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* x_originating_ip: Contents of the X-Originating-IP header.
		 */
		fields.add(Schema.Field.of("x_originating_ip", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* first_received: Contents of the first Received header.
		 */
		fields.add(Schema.Field.of("first_received", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* second_received: Contents of the second Received header.
		 */
		fields.add(Schema.Field.of("second_received", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* last_reply: The last message that the server sent to the client.
		 */
		fields.add(Schema.Field.of("last_reply", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* path: The message transmission path, as extracted from the headers.
		 */
		fields.add(Schema.Field.of("path", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));

		/* user_agent: Value of the User-Agent header from the client.
		 */
		fields.add(Schema.Field.of("user_agent", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* tls: Indicates that the connection has switched to using TLS.
		 */
		fields.add(Schema.Field.of("tls", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));

		/* fuids: An ordered vector of file unique IDs seen attached to the message.
		 */
		fields.add(Schema.Field.of("fuids", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));

		/* is_webmail: Boolean indicator of if the message was sent through a webmail interface.
		 */
		fields.add(Schema.Field.of("is_webmail", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		Schema schema = Schema.recordOf("smtp_log", fields);
		return schema;
		
	}	
	
	/*
	 * snmp (&log)
	 * 
	 * SNMP messages
	 * 
	 * {
	 * 	"ts":1543877948.916584,
	 * 	"uid":"CnKW1B4w9fpRa6Nkf2",
	 * 	"id.orig_h":"192.168.1.2",
	 * 	"id.orig_p":59696,
	 * 	"id.resp_h":"192.168.1.1",
	 * 	"id.resp_p":161,
	 * 	"duration":7.849924,
	 * 	"version":"2c",
	 * 	"community":"public",
	 * 	"get_requests":0,
	 * 	"get_bulk_requests":0,
	 * 	"get_responses":8,
	 * 	"set_requests":0,
	 * 	"up_since":1543631204.766508
	 * }
	 */
	public static StructuredRecord fromSnmp(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceTime(newObject, "up_since");

		newObject = replaceInterval(newObject, "duration");		
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, snmp());

	}
	
	private static Schema snmp() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp of first packet belonging to the SNMP session.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* duration: The amount of time between the first packet beloning to the 
		 * SNMP session and the latest one seen.
		 */
		fields.add(Schema.Field.of("duration", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* version: The version of SNMP being used.
		 */
		fields.add(Schema.Field.of("version", Schema.of(Schema.Type.STRING)));
		
		/* community: The community string of the first SNMP packet associated with the session. 
		 * This is used as part of SNMP’s (v1 and v2c) administrative/security framework. 
		 * 
		 * See RFC 1157 or RFC 1901.
		 */
		fields.add(Schema.Field.of("community", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* get_requests: The number of variable bindings in GetRequest/GetNextRequest 
		 * PDUs seen for the session.
		 */
		fields.add(Schema.Field.of("get_requests", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* get_bulk_requests: The number of variable bindings in GetBulkRequest PDUs seen 
		 * for the session.
		 */
		fields.add(Schema.Field.of("get_bulk_requests", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* get_responses: The number of variable bindings in GetResponse/Response PDUs 
		 * seen for the session.
		 */
		fields.add(Schema.Field.of("get_responses", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* set_requests: The number of variable bindings in SetRequest PDUs seen for the session.
		 */
		fields.add(Schema.Field.of("set_requests", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* display_string: A system description of the SNMP responder endpoint.
		 */
		fields.add(Schema.Field.of("display_string", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* up_since: The time at which the SNMP responder endpoint claims it’s been up since.
		 */
		fields.add(Schema.Field.of("up_since", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		Schema schema = Schema.recordOf("snmp_log", fields);
		return schema;
	
	}	
	
	/*
	 * socks (&log)
	 * 
	 * SOCKS proxy requests
	 * 
	 * {
	 * 	"ts":1566508093.09494,
	 * 	"uid":"Cmz4Cb4qCw1hGqYw1c",
	 * 	"id.orig_h":"127.0.0.1",
	 * 	"id.orig_p":35368,
	 * 	"id.resp_h":"127.0.0.1",
	 * 	"id.resp_p":8080,
	 * 	"version":5,
	 * 	"status":"succeeded",
	 * 	"request.name":"www.google.com",
	 * 	"request_p":443,
	 * 	"bound.host":"0.0.0.0",
	 * 	"bound_p":0
	 * }
	 */
	public static StructuredRecord fromSocks(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);
		
		newObject = replaceName(newObject, "request_host", "request.host");		
		newObject = replaceName(newObject, "request_name", "request.name");
		newObject = replaceName(newObject, "request_port", "request_p");
		
		newObject = replaceName(newObject, "bound_host", "bound.host");		
		newObject = replaceName(newObject, "bound_name", "bound.name");
		newObject = replaceName(newObject, "bound_port", "bound_p");		
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, socks());

	}
	
	private static Schema socks() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Time when the proxy connection was first detected.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* version: Protocol version of SOCKS.
		 */
		fields.add(Schema.Field.of("version", Schema.of(Schema.Type.INT)));
		
		/* user: Username used to request a login to the proxy.
		 */
		fields.add(Schema.Field.of("user", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* password: Password used to request a login to the proxy.
		 */
		fields.add(Schema.Field.of("password", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* status: Server status for the attempt at using the proxy.
		 */
		fields.add(Schema.Field.of("status", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/* request: Client requested SOCKS address. Could be an address, a name or both. */
		
		/* request.host: 
		 */
		fields.add(Schema.Field.of("request_host", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* request.name: 
		 */
		fields.add(Schema.Field.of("request_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* request_p: Client requested port.
		 */
		fields.add(Schema.Field.of("request_port", Schema.nullableOf(Schema.of(Schema.Type.INT))));

		/* bound: Server bound address. Could be an address, a name or both. */
		
		/* bound.host: 
		 */
		fields.add(Schema.Field.of("bound_host", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* bound.name: 
		 */
		fields.add(Schema.Field.of("bound_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* bound_p: Server bound port.
		 */
		fields.add(Schema.Field.of("bound_port", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		Schema schema = Schema.recordOf("socks_log", fields);
		return schema;
	
	}	
	
	/*
	 * ssh (&log)
	 * 
	 * {
	 * 	"ts":1562527532.904291,
	 * 	"uid":"CajWfz1b3qnnWT0BU9",
	 * 	"id.orig_h":"192.168.1.2",
	 * 	"id.orig_p":48380,
	 * 	"id.resp_h":"192.168.1.1",
	 * 	"id.resp_p":22,
	 * 	"version":2,
	 * 	"auth_success":false,
	 * 	"auth_attempts":2,
	 * 	"client":"SSH-2.0-OpenSSH_7.9p1 Ubuntu-10",
	 * 	"server":"SSH-2.0-OpenSSH_6.6.1p1 Debian-4~bpo70+1",
	 * 	"cipher_alg":"chacha20-poly1305@openssh.com",
	 * 	"mac_alg":"umac-64-etm@openssh.com",
	 * 	"compression_alg":"none",
	 * 	"kex_alg":"curve25519-sha256@libssh.org",
	 * 	"host_key_alg":"ecdsa-sha2-nistp256",
	 * 	"host_key":"86:71:ac:9c:35:1c:28:29:05:81:48:ec:66:67:de:bd"
	 * }
	 */
	public static StructuredRecord fromSsh(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);
		
		newObject = replaceName(newObject, "country_code", "remote_location.country_code");
		newObject = replaceName(newObject, "region", "remote_location.region");
		newObject = replaceName(newObject, "city", "remote_location.city");

		newObject = replaceName(newObject, "latitude", "remote_location.latitude");
		newObject = replaceName(newObject, "longitude", "remote_location.longitude");
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, ssh());
		
	}
	
	private static Schema ssh() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Time when the SSH connection began.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* version: SSH major version (1 or 2)
		 */
		fields.add(Schema.Field.of("version", Schema.of(Schema.Type.INT)));
		
		/* auth_success: Authentication result (T=success, F=failure, unset=unknown)
		 */
		fields.add(Schema.Field.of("auth_success", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* auth_attempts: The number of authentication attemps we observed. There’s always at least 
		 * one, since some servers might support no authentication at all. It’s important to note that 
		 * not all of these are failures, since some servers require two-factor auth (e.g. password AND 
		 * pubkey)
		 */
		fields.add(Schema.Field.of("auth_attempts", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* direction: Direction of the connection. If the client was a local host logging into an 
		 * external host, this would be OUTBOUND. INBOUND would be set for the opposite situation.
		 */
		fields.add(Schema.Field.of("direction", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* client: The client’s version string.
		 */
		fields.add(Schema.Field.of("client", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* server: The server’s version string.
		 */
		fields.add(Schema.Field.of("server", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* cipher_alg: The encryption algorithm in use.
		 */
		fields.add(Schema.Field.of("cipher_alg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* mac_alg: The signing (MAC) algorithm in use.
		 */
		fields.add(Schema.Field.of("mac_alg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* compression_alg: The compression algorithm in use.
		 */
		fields.add(Schema.Field.of("compression_alg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* kex_alg: The key exchange algorithm in use.
		 */
		fields.add(Schema.Field.of("kex_alg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* host_key_alg: The server host key’s algorithm.
		 */
		fields.add(Schema.Field.of("host_key_alg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* host_key: The server’s key fingerprint.
		 */
		fields.add(Schema.Field.of("host_key", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

		/*** remote_location: Add geographic data related to the “remote” host of the connection. ***/
		
		/* remote_location.country_code: The country code.
		 */
		fields.add(Schema.Field.of("country_code", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* remote_location.region: The The region.
		 */
		fields.add(Schema.Field.of("region", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* remote_location.city: The city.
		 */
		fields.add(Schema.Field.of("city", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* remote_location.latitude: The latitude.
		 */
		fields.add(Schema.Field.of("latitude", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
		
		/* remote_location.longitude: longitude.
		 */
		fields.add(Schema.Field.of("longitude", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
		
		Schema schema = Schema.recordOf("ssh_log", fields);		
		return schema;
	
	}	
	
	/*
	 * ssl (&log)
	 * 
	 * This logs information about the SSL/TLS handshaking 
	 * and encryption establishment process.
	 * 
	 * {
	 *  "ts":1547688736.805088,
	 * 	"uid":"CAOvs1BMFCX2Eh0Y3",
	 * 	"id.orig_h":"10.178.98.102",
	 * 	"id.orig_p":63199,
	 * 	"id.resp_h":"35.199.178.4",
	 * 	"id.resp_p":9243,
	 * 	"version":"TLSv12",
	 * 	"cipher":"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
	 * 	"curve":"secp256r1",
	 * 	"server_name":"dd625ffb4fc54735b281862aa1cd6cd4.us-west1.gcp.cloud.es.io",
	 * 	"resumed":false,
	 * 	"established":true,
	 * 	"cert_chain_fuids":["FebkbHWVCV8rEEEne","F4BDY41MGUBT6URZMd","FWlfEfiHVkv8evDL3"],
	 * 	"client_cert_chain_fuids":[],
	 * 	"subject":"CN=*.gcp.cloud.es.io,O=Elasticsearch\u005c, Inc.,L=Mountain View,ST=California,C=US",
	 * 	"issuer":"CN=DigiCert SHA2 Secure Server CA,O=DigiCert Inc,C=US",
	 * 	"validation_status":"ok"
	 * }
	 */
	public static StructuredRecord fromSsl(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);
		
		newObject = replaceName(newObject, "notary_first_seen", "notary.first_seen");
		newObject = replaceName(newObject, "notary_last_seen", "notary.last_seen");
		
		newObject = replaceName(newObject, "notary_times_seen", "notary.times_seen");
		newObject = replaceName(newObject, "notary_valid", "notary.valid");
				
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, ssl());

	}
	
	private static Schema ssl() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Time when the SSL connection was first detected.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* version: SSL/TLS version that the server chose.
		 */
		fields.add(Schema.Field.of("version", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* cipher: SSL/TLS cipher suite that the server chose.
		 */
		fields.add(Schema.Field.of("cipher", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* curve: Elliptic curve the server chose when using ECDH/ECDHE.
		 */
		fields.add(Schema.Field.of("curve", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* server_name: Value of the Server Name Indicator SSL/TLS extension. 
		 * It indicates the server name that the client was requesting.
		 */
		fields.add(Schema.Field.of("server_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* resumed: Flag to indicate if the session was resumed reusing the 
		 * key material exchanged in an earlier connection.
		 */
		fields.add(Schema.Field.of("resumed", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* last_alert: Last alert that was seen during the connection.
		 */
		fields.add(Schema.Field.of("last_alert", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* next_protocol: Next protocol the server chose using the application 
		 * layer next protocol extension, if present.
		 */
		fields.add(Schema.Field.of("next_protocol", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* established: Flag to indicate if this ssl session has been established 
		 * successfully, or if it was aborted during the handshake.
		 */
		fields.add(Schema.Field.of("established", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* cert_chain_fuids: An ordered vector of all certificate file unique IDs 
		 * for the certificates offered by the server.
		 */
		fields.add(Schema.Field.of("cert_chain_fuids", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* client_cert_chain_fuids: An ordered vector of all certificate file unique IDs 
		 * for the certificates offered by the client.
		 */
		fields.add(Schema.Field.of("client_cert_chain_fuids", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* subject: Subject of the X.509 certificate offered by the server.
		 */
		fields.add(Schema.Field.of("subject", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* issuer: Subject of the signer of the X.509 certificate offered by the server.
		 */
		fields.add(Schema.Field.of("issuer", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* client_subject: Subject of the X.509 certificate offered by the client.
		 */
		fields.add(Schema.Field.of("client_subject", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* client_issuer: Subject of the signer of the X.509 certificate offered by the client.
		 */
		fields.add(Schema.Field.of("client_issuer", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* validation_status: Result of certificate validation for this connection.
		 */
		fields.add(Schema.Field.of("validation_status", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* ocsp_status: Result of ocsp validation for this connection.
		 */
		fields.add(Schema.Field.of("ocsp_status", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* valid_ct_logs: Number of different Logs for which valid SCTs were 
		 * encountered in the connection.
		 */
		fields.add(Schema.Field.of("valid_ct_logs", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* valid_ct_operators: Number of different Log operators of which valid 
		 * SCTs were encountered in the connection.
		 */
		fields.add(Schema.Field.of("valid_ct_operators", Schema.nullableOf(Schema.of(Schema.Type.INT))));

		/*** notary: A response from the ICSI certificate notary. ***/
		
		/* notary.first_seen: 
		 */
		fields.add(Schema.Field.of("notary_first_seen", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* notary.last_seen: 
		 */
		fields.add(Schema.Field.of("notary_last_seen", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* notary.times_seen: 
		 */
		fields.add(Schema.Field.of("notary_times_seen", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* notary.valid: 
		 */
		fields.add(Schema.Field.of("notary_valid", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		Schema schema = Schema.recordOf("ssl_log", fields);		
		return schema;
	
	}	
	
	/*
	 * stats (&log)
	 * 
	 * {
	 * 	"ts":1476605878.714844,
	 * 	"peer":"bro",
	 * 	"mem":94,
	 * 	"pkts_proc":296,
	 * 	"bytes_recv":39674,
	 * 	"events_proc":723,
	 * 	"events_queued":728,
	 * 	"active_tcp_conns":1,
	 * 	"active_udp_conns":3,
	 * 	"active_icmp_conns":0,
	 * 	"tcp_conns":6,
	 * 	"udp_conns":36,
	 * 	"icmp_conns":2,
	 * 	"timers":797,
	 * 	"active_timers":38,
	 * 	"files":0,
	 * 	"active_files":0,
	 * 	"dns_requests":0,
	 * 	"active_dns_requests":0,
	 * 	"reassem_tcp_size":0,
	 * 	"reassem_file_size":0,
	 * 	"reassem_frag_size":0,
	 * 	"reassem_unknown_size":0
	 * }
	 */
	public static StructuredRecord fromStats(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceInterval(newObject, "pkt_lag");
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, stats());
		
	}
	
	private static Schema stats() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for the measurement.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* peer: Peer that generated this log. Mostly for clusters.
		 */
		fields.add(Schema.Field.of("peer", Schema.of(Schema.Type.STRING)));
		
		/* mem: Amount of memory currently in use in MB.
		 */
		fields.add(Schema.Field.of("mem", Schema.of(Schema.Type.INT)));
		
		/* pkts_proc: Number of packets processed since the last stats interval.
		 */
		fields.add(Schema.Field.of("pkts_proc", Schema.of(Schema.Type.INT)));
		
		/* bytes_recv: Number of bytes received since the last stats interval
		 * if reading live traffic.
		 */
		fields.add(Schema.Field.of("bytes_recv", Schema.of(Schema.Type.INT)));
		
		/* pkts_dropped: Number of packets dropped since the last stats 
		 * interval if reading live traffic.
		 */
		fields.add(Schema.Field.of("pkts_dropped", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* pkts_link: Number of packets seen on the link since the last 
		 * stats interval if reading live traffic.
		 */
		fields.add(Schema.Field.of("pkts_link", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* pkt_lag: Lag between the wall clock and packet timestamps 
		 * if reading live traffic.
		 */
		fields.add(Schema.Field.of("pkt_lag", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
		
		/* events_proc: Number of events processed since the last stats 
		 * interval.
		 */
		fields.add(Schema.Field.of("events_proc", Schema.of(Schema.Type.INT)));
		
		/* events_queued: Number of events that have been queued since 
		 * the last stats interval.
		 */
		fields.add(Schema.Field.of("events_queued", Schema.of(Schema.Type.INT)));
		
		/* active_tcp_conns: TCP connections currently in memory.
		 */
		fields.add(Schema.Field.of("active_tcp_conns", Schema.of(Schema.Type.INT)));
		
		/* active_udp_conns: UDP connections currently in memory.
		 */
		fields.add(Schema.Field.of("active_udp_conns", Schema.of(Schema.Type.INT)));
		
		/* active_icmp_conns: ICMP connections currently in memory.
		 */
		fields.add(Schema.Field.of("active_icmp_conns", Schema.of(Schema.Type.INT)));
		
		/* tcp_conns: TCP connections seen since last stats interval.
		 */
		fields.add(Schema.Field.of("tcp_conns", Schema.of(Schema.Type.INT)));
		
		/* udp_conns: UDP connections seen since last stats interval.
		 */
		fields.add(Schema.Field.of("udp_conns", Schema.of(Schema.Type.INT)));
		
		/* icmp_conns: ICMP connections seen since last stats interval.
		 */
		fields.add(Schema.Field.of("icmp_conns", Schema.of(Schema.Type.INT)));
		
		/* timers: Number of timers scheduled since last stats interval.
		 */
		fields.add(Schema.Field.of("timers", Schema.of(Schema.Type.INT)));
		
		/* active_timers: Current number of scheduled timers.
		 */
		fields.add(Schema.Field.of("active_timers", Schema.of(Schema.Type.INT)));
		
		/* files: Number of files seen since last stats interval.
		 */
		fields.add(Schema.Field.of("files", Schema.of(Schema.Type.INT)));
		
		/* active_files: Current number of files actively being seen.
		 */
		fields.add(Schema.Field.of("active_files", Schema.of(Schema.Type.INT)));
		
		/* dns_requests: Number of DNS requests seen since last stats interval.
		 */
		fields.add(Schema.Field.of("dns_requests", Schema.of(Schema.Type.INT)));
		
		/* active_dns_requests: Current number of DNS requests awaiting a reply.
		 */
		fields.add(Schema.Field.of("active_dns_requests", Schema.of(Schema.Type.INT)));
		
		/* reassem_tcp_size: Current size of TCP data in reassembly.
		 */
		fields.add(Schema.Field.of("reassem_tcp_size", Schema.of(Schema.Type.INT)));
		
		/* reassem_file_size: Current size of File data in reassembly.
		 */
		fields.add(Schema.Field.of("reassem_file_size", Schema.of(Schema.Type.INT)));
		
		/* reassem_frag_size: Current size of packet fragment data in reassembly.
		 */
		fields.add(Schema.Field.of("reassem_frag_size", Schema.of(Schema.Type.INT)));
		
		/* reassem_unknown_size: Current size of unknown data in reassembly 
		 * (this is only PIA buffer right now).
		 */
		fields.add(Schema.Field.of("reassem_unknown_size", Schema.of(Schema.Type.INT)));
		
		Schema schema = Schema.recordOf("stats_log", fields);		
		return schema;
		
	}
	
	/*
	 * syslog (&log)
	 * 
	 * Syslog messages.
	 */
	public static StructuredRecord fromSyslog(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, syslog());

	}
	
	private static Schema syslog() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp when the syslog message was seen.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: A unique identifier of the connection. 
		 */
		fields.add(Schema.Field.of("uid", Schema.of(Schema.Type.STRING)));
		
		/* id
		 */
		fields.addAll(conn_id());
		
		/* proto: Protocol over which the message was seen. 
		 */
		fields.add(Schema.Field.of("proto", Schema.of(Schema.Type.STRING)));
		
		/* facility: Syslog facility for the message.
		 */
		fields.add(Schema.Field.of("facility", Schema.of(Schema.Type.STRING)));
		
		/* severity: Syslog severity for the message.
		 */
		fields.add(Schema.Field.of("severity", Schema.of(Schema.Type.STRING)));
		
		/* message: The plain text message.
		 */
		fields.add(Schema.Field.of("message", Schema.of(Schema.Type.STRING)));
		
		Schema schema = Schema.recordOf("syslog_log", fields);
		return schema;
	
	}	
	
	/*
	 * traceroute (&log)
	 * 
	 * {
	 * 	"ts":1361916158.650605,
	 * 	"src":"192.168.1.1",
	 * 	"dst":"8.8.8.8",
	 * 	"proto":"udp"
	 * }
	 */
	public static StructuredRecord fromTraceroute(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		
		newObject = replaceName(newObject, "source_ip", "src");		
		newObject = replaceName(newObject, "destination_ip", "dst");		
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, traceroute());
		
	}
	
	private static Schema traceroute() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* src: Address initiating the traceroute.
		 */
		fields.add(Schema.Field.of("source_ip", Schema.of(Schema.Type.STRING)));
		
		/* dst: Destination address of the traceroute.
		 */
		fields.add(Schema.Field.of("destination_ip", Schema.of(Schema.Type.STRING)));
		
		/* proto: Protocol used for the traceroute.
		 */
		fields.add(Schema.Field.of("proto", Schema.of(Schema.Type.STRING)));
		
		Schema schema = Schema.recordOf("traceroute_log", fields);
		return schema;
		
	}
	/*
	 * tunnel (&log)
	 * 
	 * This log handles the tracking/logging of tunnels (e.g. Teredo, AYIYA, or IP-in-IP 
	 * such as 6to4 where “IP” is either IPv4 or IPv6).
	 * 
	 * For any connection that occurs over a tunnel, information about its encapsulating 
	 * tunnels is also found in the tunnel field of connection.
	 * 
	 * {
	 * 	"ts":1544405666.743509,
	 * 	"id.orig_h":"132.16.146.79",
	 * 	"id.orig_p":0,
	 * 	"id.resp_h":"132.16.110.133",
	 * 	"id.resp_p":8080,
	 * 	"tunnel_type":"Tunnel::HTTP",
	 * 	"action":"Tunnel::DISCOVER"
	 * }
	 */
	public static StructuredRecord fromTunnel(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, tunnel());
		
	}
	
	private static Schema tunnel() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Time at which some tunnel activity occurred.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: The unique identifier for the tunnel, which may correspond to a connection’s uid 
		 * field for non-IP-in-IP tunnels. This is optional because there could be numerous 
		 * connections for payload proxies like SOCKS but we should treat it as a single tunnel.
		 */
		fields.add(Schema.Field.of("uid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* id: The tunnel “connection” 4-tuple of endpoint addresses/ports. For an IP tunnel, 
		 * the ports will be 0.
		 */
		fields.addAll(conn_id());
		
		/* tunnel_type: Time at which some tunnel activity occurred.
		 */
		fields.add(Schema.Field.of("tunnel_type", Schema.of(Schema.Type.STRING)));
		
		/* action: The type of activity that occurred.
		 */
		fields.add(Schema.Field.of("action", Schema.of(Schema.Type.STRING)));
		
		Schema schema = Schema.recordOf("tunnel_log", fields);
		return schema;
	
	}	
	/*
	 * weird (&log)
	 * 
	 * {
	 * 	"ts":1543877999.99354,
	 * 	"uid":"C1ralPp062bkwWt4e",
	 * 	"id.orig_h":"192.168.1.1",
	 * 	"id.orig_p":64521,
	 * 	"id.resp_h":"192.168.1.2",
	 * 	"id.resp_p":53,
	 * 	"name":"dns_unmatched_reply",
	 * 	"notice":false,
	 * 	"peer":"worker-6"
	 * }
	 * 
	 * This log provides a default set of actions to take for “weird activity” events generated 
	 * from Zeek’s event engine. Weird activity is defined as unusual or exceptional activity that 
	 * can indicate malformed connections, traffic that doesn’t conform to a particular protocol, 
	 * malfunctioning or misconfigured hardware, or even an attacker attempting to avoid/confuse 
	 * a sensor. 
	 * 
	 * Without context, it’s hard to judge whether a particular category of weird activity is interesting, 
	 * but this script provides a starting point for the user.
	 */
	public static StructuredRecord fromWeird(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceConnId(newObject);
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, weird());
		
	}
	
	private static Schema weird() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Timestamp for when the weird occurred.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* uid: If a connection is associated with this weird, this will be the 
		 * connection’s unique ID. 
		 */
		fields.add(Schema.Field.of("uid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* id
		 */
		fields.addAll(conn_id_nullable());
		
		/* name: The name of the weird that occurred.
		 */
		fields.add(Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
		
		/* addl: Additional information accompanying the weird if any.
		 */
		fields.add(Schema.Field.of("addl", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* notice: Indicate if this weird was also turned into a notice.
		 */
		fields.add(Schema.Field.of("notice", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* peer: The peer that originated this weird. This is helpful in cluster 
		 * deployments if a particular cluster node is having trouble to help identify 
		 * which node is having trouble.
		 */
		fields.add(Schema.Field.of("peer", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		Schema schema = Schema.recordOf("weird_log", fields);
		return schema;
		
	}
	/*
	 * x509 (&log)
	 * 
	 * {
	 * 	"ts":1543867200.143484,
	 * 	"id":"FxZ6gZ3YR6vFlIocq3",
	 * 	"certificate.version":3,
	 * 	"certificate.serial":"2D00003299D7071DB7D1708A42000000003299",
	 * 	"certificate.subject":"CN=www.bing.com",
	 * 	"certificate.issuer":"CN=Microsoft IT TLS CA 5,OU=Microsoft IT,O=Microsoft Corporation,L=Redmond,ST=Washington,C=US",
	 * 	"certificate.not_valid_before":1500572828.0,
	 * 	"certificate.not_valid_after":1562780828.0,
	 * 	"certificate.key_alg":"rsaEncryption",
	 * 	"certificate.sig_alg":"sha256WithRSAEncryption",
	 * 	"certificate.key_type":"rsa",
	 * 	"certificate.key_length":2048,
	 * 	"certificate.exponent":"65537",
	 * 	"san.dns":["www.bing.com","dict.bing.com.cn","*.platform.bing.com","*.bing.com","bing.com","ieonline.microsoft.com","*.windowssearch.com","cn.ieonline.microsoft.com","*.origin.bing.com","*.mm.bing.net","*.api.bing.com","ecn.dev.virtualearth.net","*.cn.bing.net","*.cn.bing.com","ssl-api.bing.com","ssl-api.bing.net","*.api.bing.net","*.bingapis.com","bingsandbox.com","feedback.microsoft.com","insertmedia.bing.office.net","r.bat.bing.com","*.r.bat.bing.com","*.dict.bing.com.cn","*.dict.bing.com","*.ssl.bing.com","*.appex.bing.com","*.platform.cn.bing.com","wp.m.bing.com","*.m.bing.com","global.bing.com","windowssearch.com","search.msn.com","*.bingsandbox.com","*.api.tiles.ditu.live.com","*.ditu.live.com","*.t0.tiles.ditu.live.com","*.t1.tiles.ditu.live.com","*.t2.tiles.ditu.live.com","*.t3.tiles.ditu.live.com","*.tiles.ditu.live.com","3d.live.com","api.search.live.com","beta.search.live.com","cnweb.search.live.com","dev.live.com","ditu.live.com","farecast.live.com","image.live.com","images.live.com","local.live.com.au","localsearch.live.com","ls4d.search.live.com","mail.live.com","mapindia.live.com","local.live.com","maps.live.com","maps.live.com.au","mindia.live.com","news.live.com","origin.cnweb.search.live.com","preview.local.live.com","search.live.com","test.maps.live.com","video.live.com","videos.live.com","virtualearth.live.com","wap.live.com","webmaster.live.com","webmasters.live.com","www.local.live.com.au","www.maps.live.com.au"]
	 * }
	 *
	 */
	public static StructuredRecord fromX509(JsonObject oldObject) throws IOException {

		JsonObject newObject = oldObject;
		/*
		 * Prepare JsonObject, i.e. rename fields and 
		 * transform time values
		 */
		newObject = replaceTime(newObject, "ts");
		newObject = replaceCertificate(newObject);
		
		newObject = replaceName(newObject, "san_dns", "san.dns");		
		newObject = replaceName(newObject, "san_uri", "san.uri");		
		
		newObject = replaceName(newObject, "san_email", "san.email");		
		newObject = replaceName(newObject, "san_ip", "san.ip");		
		
		newObject = replaceName(newObject, "san_other_fields", "san.other_fields");		
		
		newObject = replaceName(newObject, "basic_constraints_ca", "basic_constraints.ca");		
		newObject = replaceName(newObject, "basic_constraints_path_len", "basic_constraints.path_len");		
		
		/* Retrieve structured record */
		String json = newObject.toString();
		
		return StructuredRecordStringConverter.fromJsonString(json, x509());

	}
	
	private static Schema x509() {
		
		List<Field> fields = new ArrayList<>();
		
		/* ts: Current timestamp.
		 */
		fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
		
		/* id: File id of this certificate. 
		 */
		fields.add(Schema.Field.of("id", Schema.of(Schema.Type.STRING)));

		/*** CERTIFICATE DESCRIPTION ***/
		
		/* certificate: Basic information about the certificate.
		 */
		fields.addAll(certificate());
		
		/* san.dns: List of DNS entries in SAN (Subject Alternative Name)
		 */
		fields.add(Schema.Field.of("san_dns", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* san.uri: List of URI entries in SAN
		 */
		fields.add(Schema.Field.of("san_uri", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* san.email: List of email entries in SAN
		 */
		fields.add(Schema.Field.of("san_email", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* san.ip: List of IP entries in SAN
		 */
		fields.add(Schema.Field.of("san_ip", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));
		
		/* san.other_fields: True if the certificate contained other, not recognized or parsed name fields.
		 */
		fields.add(Schema.Field.of("san_other_fields", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));

		/*** BASIC CONSTRAINTS ***/
		
		/* basic_constraints.ca: CA flag set?
		 */
		fields.add(Schema.Field.of("basic_constraints_ca", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
		
		/* basic_constraints.path_len: Maximum path length.
		 */
		fields.add(Schema.Field.of("basic_constraints_path_len", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		Schema schema = Schema.recordOf("x509_log", fields);
		return schema;
		
	}
	
	/********************
	 * 
	 * BASE SCHEMAS
	 */
	
	public static List<Schema.Field> certificate() {
		
		List<Field> fields = new ArrayList<>();
		
		/* certificate.version: Version number. 
		 */
		fields.add(Schema.Field.of("cert_version", Schema.of(Schema.Type.INT)));
		
		/* certificate.serial: Serial number.
		 */
		fields.add(Schema.Field.of("cert_serial", Schema.of(Schema.Type.STRING)));
		
		/* certificate.subject: Subject.
		 */
		fields.add(Schema.Field.of("cert_subject", Schema.of(Schema.Type.STRING)));
		
		/* certificate.cn: Last (most specific) common name.
		 */
		fields.add(Schema.Field.of("cert_cn", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* certificate.not_valid_before: Timestamp before when certificate is not valid.
		 */
		fields.add(Schema.Field.of("cert_not_valid_before", Schema.of(Schema.Type.LONG)));
		
		/* certificate.not_valid_after: Timestamp after when certificate is not valid.
		 */
		fields.add(Schema.Field.of("cert_not_valid_after", Schema.of(Schema.Type.LONG)));
		
		/* certificate.key_alg: Name of the key algorithm.
		 */
		fields.add(Schema.Field.of("cert_key_alg", Schema.of(Schema.Type.STRING)));
		
		/* certificate.sig_alg: Name of the signature algorithm.
		 */
		fields.add(Schema.Field.of("cert_sig_alg", Schema.of(Schema.Type.STRING)));
		
		/* certificate.key_type: Key type, if key parseable by openssl (either rsa, dsa or ec).
		 */
		fields.add(Schema.Field.of("cert_key_type", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* certificate.key_length: Key length in bits.
		 */
		fields.add(Schema.Field.of("cert_key_length", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* certificate.exponent: Exponent, if RSA-certificate.
		 */
		fields.add(Schema.Field.of("cert_exponent", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* certificate.curve: Curve, if EC-certificate.
		 */
		fields.add(Schema.Field.of("cert_curve", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		return fields;
		
	}
	
	
	public static List<Schema.Field> conn_id() {
		
		List<Field> fields = new ArrayList<>();
		
		/* id.orig_h: The originator’s IP address. 
		 */
		fields.add(Schema.Field.of("source_ip", Schema.of(Schema.Type.STRING)));
		
		/* id.orig_p: The originator’s port number. 
		 */
		fields.add(Schema.Field.of("source_port", Schema.of(Schema.Type.INT)));
		
		/* id.resp_h: The responder’s IP address. 
		 */
		fields.add(Schema.Field.of("destination_ip", Schema.of(Schema.Type.STRING)));
		
		/* id.resp_p: The responder’s port number.
		 */
		fields.add(Schema.Field.of("destination_port", Schema.of(Schema.Type.INT)));
		
		return fields;

	}
	
	public static List<Schema.Field> conn_id_nullable() {
		
		List<Field> fields = new ArrayList<>();
		
		/* id.orig_h: The originator’s IP address. 
		 */
		fields.add(Schema.Field.of("source_ip", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* id.orig_p: The originator’s port number. 
		 */
		fields.add(Schema.Field.of("source_port", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		/* id.resp_h: The responder’s IP address. 
		 */
		fields.add(Schema.Field.of("destination_ip", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
		
		/* id.resp_p: The responder’s port number.
		 */
		fields.add(Schema.Field.of("destination_port", Schema.nullableOf(Schema.of(Schema.Type.INT))));
		
		return fields;

	}
	
	private static JsonObject replaceCertificate(JsonObject oldObject) {

		JsonObject newObject = oldObject;
		
		newObject = replaceTime(newObject, "certificate.not_valid_before");
		newObject = replaceTime(newObject, "certificate.not_valid_after");
		
		newObject = replaceName(newObject, "cert_version", "certificate.version");
		newObject = replaceName(newObject, "cert_serial", "certificate.serial");

		newObject = replaceName(newObject, "cert_subject", "certificate.subject");
		newObject = replaceName(newObject, "cert_cn", "certificate.cn");

		newObject = replaceName(newObject, "cert_not_valid_before", "certificate.not_valid_before");
		newObject = replaceName(newObject, "cert_not_valid_after", "certificate.not_valid_after");

		newObject = replaceName(newObject, "cert_key_alg", "certificate.key_alg");
		newObject = replaceName(newObject, "cert_sig_alg", "certificate.sig_alg");

		newObject = replaceName(newObject, "cert_key_type", "certificate.key_type");
		newObject = replaceName(newObject, "cert_key_length", "certificate.key_length");

		newObject = replaceName(newObject, "cert_exponent", "certificate.exponent");
		newObject = replaceName(newObject, "cert_curve", "certificate.curve");
		
		return newObject;
		
	}
	
	private static JsonObject replaceName(JsonObject jsonObject, String newName, String oldName) {
		
		if (jsonObject == null || jsonObject.get(oldName) == null) return jsonObject;
		JsonElement value = jsonObject.remove(oldName);
		
		jsonObject.add(newName, value);
		return jsonObject;
		
	}
	/*
	 * Zeek specifies timestamps (absolute time) as Double 
	 * that defines seconds; this method transforms them
	 * into milliseconds
	 */
	private static JsonObject replaceTime(JsonObject jsonObject, String timeName) {

		if (jsonObject == null || jsonObject.get(timeName) == null) return jsonObject;
	      
		long timestamp = 0L;
		try {
			
			Double ts = jsonObject.get(timeName).getAsDouble();
			timestamp = (long) (ts * 1000);
			
		} catch (Exception e) {
			;
		}

		jsonObject.remove(timeName);
		jsonObject.addProperty(timeName, timestamp);
		
		return jsonObject;
		
	}
	/*
	 * Zeek specifies intervals (relative time) as Double 
	 * that defines seconds; this method transforms them
	 * into milliseconds
	 */
	private static JsonObject replaceInterval(JsonObject jsonObject, String intervalName) {

		if (jsonObject == null || jsonObject.get(intervalName) == null) return jsonObject;
	      
		long interval = 0L;
		try {
			
			Double ts = jsonObject.get(intervalName).getAsDouble();
			interval = (long) (ts * 1000);
			
		} catch (Exception e) {
			;
		}

		jsonObject.remove(intervalName);
		jsonObject.addProperty(intervalName, interval);
		
		return jsonObject;
		
	}

	private static JsonObject replaceConnId(JsonObject oldObject) {

		JsonObject newObject = oldObject;

		newObject = replaceName(newObject, "source_ip", "id.orig_h");
		newObject = replaceName(newObject, "source_port", "id.orig_p");

		newObject = replaceName(newObject, "destination_ip", "id.resp_h");
		newObject = replaceName(newObject, "destination_port", "id.resp_p");

		return newObject;

	}
}
