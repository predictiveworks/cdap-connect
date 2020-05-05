package de.kp.works.connect.netflow;
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

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import io.netty.buffer.ByteBuf;

public class Netflow5Decoder {

	private static final int V5_HEADER_SIZE = 24;
	private static final int V5_FLOW_SIZE = 48;

	@SuppressWarnings("unused")
	private NetflowMode mode;

	private boolean readHeader = false;
	private int count = 0;

	private String readerId = null;
	private long uptime = 0;

	private long seconds = 0;
	private long nanos = 0;
	private long millis = 0;

	private long timestamp = 0;
	private UUID packetId = null;

	private long flowSequence = 0;

	private short engineType = 0;
	private short engineId = 0;

	private int sampling = 0;
	private int samplingInterval = 0;
	private int samplingMode = 0;

	private int readIndex = 0;
	private List<Netflow5Message> result = new LinkedList<>();

	public Netflow5Decoder(NetflowMode mode) {
		this.mode = mode;
	}

	public List<Netflow5Message> parse(int netflowVersion, int packetLength, boolean packetLengthCheck, ByteBuf buffer,
			InetSocketAddress sender, InetSocketAddress recipient) throws Exception {

		/***** HEADER PROCESSING *****/

		if (!readHeader) {
			/* 2-3 */
			count = buffer.readUnsignedShort();
			if (count <= 0) {
				throw new Exception(String.format("Count is invalid: %s", count));

			} else if (packetLengthCheck && packetLength < V5_HEADER_SIZE + count * V5_FLOW_SIZE) {

				String exception = String.format("Readable bytes %s are too small for count %s (max %s).", packetLength,
						count, (V5_HEADER_SIZE + count * V5_FLOW_SIZE));

				throw new Exception(exception);

			}

			if (recipient != null)
				readerId = String.valueOf(recipient);

			/* 4-7 */
			uptime = buffer.readUnsignedInt();

			/* 8-11 */
			seconds = buffer.readUnsignedInt();

			/* 12-15 */
			nanos = buffer.readUnsignedInt();

			millis = nanos / 1000000;

			/* java timestamp, which is milliseconds */
			timestamp = (seconds * 1000L) + millis;
			packetId = UUIDUtil.startOfJavaTimestamp(timestamp);

			/* 16-19 */
			flowSequence = buffer.readUnsignedInt();

			/* 20 */
			engineType = buffer.readUnsignedByte();

			/* 21 */
			engineId = buffer.readUnsignedByte();

			/*
			 * the first 2 bits are the sampling mode, the remaining 14 the interval
			 * 
			 * 22-23
			 */
			sampling = buffer.readUnsignedShort();

			samplingInterval = sampling & 0x3FFF;
			samplingMode = sampling >> 14;

			readHeader = true;

		}

		/***** MESSAGES *****/

		while (readIndex < count) {

			Netflow5Message message = new Netflow5Message();
			message.setCount(count);

			message.setSeconds(seconds);
			message.setNanos(nanos);

			/* 0 */
			int srcaddr = (int) buffer.readUnsignedInt();
			/* 4 */
			int dstaddr = (int) buffer.readUnsignedInt();
			/* 8 */
			int nexthop = (int) buffer.readUnsignedInt();
			/* 12 */
			message.setSnmpInput(buffer.readUnsignedShort());
			/* 14 */
			message.setSnmpOutput(buffer.readUnsignedShort());

			message.setPacketId(packetId.toString());
			message.setPacketLength(packetLength);

			message.setUptime(uptime);
			message.setTimestamp(timestamp);

			message.setEngineId((int) engineId);
			message.setEngineType((int) engineType);

			message.setFlowSeq(flowSequence);

			message.setSamplingRaw(sampling);
			message.setSamplingInt(samplingInterval);
			message.setSamplingMode(samplingMode);

			message.setSender((sender == null) ? "unknown" : sender.getAddress().toString());
			message.setReaderId((readerId == null) ? "unknown" : readerId);

			/* 16 */
			long packets = buffer.readUnsignedInt();
			/* 20 */
			long octets = buffer.readUnsignedInt();
			/* 24 */
			long first = buffer.readUnsignedInt();
			message.setFirstRaw(first);

			if (first > 0) {
				message.setFirst(timestamp - uptime + first);

			} else {
				message.setFirst(0L);

			}

			/* 28 */
			long last = buffer.readUnsignedInt();
			message.setLastRaw(last);

			if (last > 0) {
				message.setLast(timestamp - uptime + last);

			} else {
				message.setLast(0L);

			}

			message.setId(UUIDUtil.timeBased().toString());

			message.setSrcAddr(srcaddr);
			message.setDstAddr(dstaddr);

			message.setNextHop(nexthop);

			message.setSrcAddrAsIP(ipV4ToString(srcaddr));
			message.setDstAddrAsIP(ipV4ToString(dstaddr));
			message.setNextHopAsIP(ipV4ToString(nexthop));

			/* 32 */
			message.setSrcPort(buffer.readUnsignedShort());

			/* 34 */
			message.setDstPort(buffer.readUnsignedShort());

			/* 36 is "pad1" (unused zero bytes) */
			buffer.readByte();

			/* 37 */
			message.setTcpFlags((int) buffer.readUnsignedByte());

			/* 38 */
			message.setProto((int) buffer.readUnsignedByte());

			/* 39 */
			message.setTos((int) buffer.readUnsignedByte());

			/* 40 */
			message.setSrcAs((int) buffer.readUnsignedShort());

			/* 42 */
			message.setDstAs((int) buffer.readUnsignedShort());

			/* 44 */
			message.setSrcMask((int) buffer.readUnsignedByte());

			/* 45 */
			message.setDstMask((int) buffer.readUnsignedByte());

			message.setPackets(packets);
			message.setDOctets(octets);

			/* 46-47 is "pad2" (unused zero bytes) */
			buffer.skipBytes(2);
			result.add(message);

			readIndex++;

		}

		/*
		 * If we reached this point without any further Signal errors, we have finished
		 * consuming
		 */
		List<Netflow5Message> output = new LinkedList<>(result);
		resetState();

		return output;

	}

	private static String ipV4ToString(int ip) {
		return String.format("%d.%d.%d.%d", (ip >> 24 & 0xff), (ip >> 16 & 0xff), (ip >> 8 & 0xff), (ip & 0xff));
	}

	private void resetState() {

		readHeader = false;
		count = 0;
		uptime = 0;
		seconds = 0;
		nanos = 0;
		millis = 0;
		timestamp = 0;
		flowSequence = 0;
		engineType = 0;
		engineId = 0;
		sampling = 0;
		samplingInterval = 0;
		samplingMode = 0;
		packetId = null;
		readerId = null;
		readIndex = 0;

		result.clear();
		
	}

}
