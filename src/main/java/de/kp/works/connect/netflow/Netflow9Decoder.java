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

import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBuf;

public class Netflow9Decoder {

	private static final int V9_HEADER_SIZE = 20;

	private NetflowMode mode;

	private boolean readHeader = false;

	private Integer count = null;

	private Long sysUptimeMs = null;
	private Long unixSeconds = null;

	private Long packetSequenceNum = null;
	private byte[] sourceIdBytes = null;
	private long sourceId = 0;

	private byte[] currentRawBytes = null;
	private Integer currentRawBytesIndex = null;

	private long timestamp = 0;

	private short engineType = 0;
	private short engineId = 0;
	private int sampling = 0;
	private int samplingInterval = 0;
	private int samplingMode = 0;
	private UUID packetId = null;

	private String readerId = null;

	/* Number of fields in this template */
	private Integer currentTemplateFieldCount = null;

	private int readIndex = 0;
	private List<Netflow9Message> result = new LinkedList<>();

	public Netflow9Decoder(NetflowMode mode) {
		this.mode = mode;
	}

	public List<Netflow9Message> parse(int netflowVersion, int packetLength, boolean packetLengthCheck, ByteBuf buffer,
			InetSocketAddress sender, InetSocketAddress recipient) throws Exception {

		/***** HEADER PROCESSING *****/

		if (!readHeader) {

			if (count == null)
				/* 2-3 */
				count = buffer.readUnsignedShort();

			if (count <= 0)
				throw new Exception(String.format("Count is invalid: %s", count));

			/*
			 * We cannot perform the packet length validation in Netflow v9, since the set
			 * records (template and data) can be variable sizes, so we don't know up front
			 * if there are enough bytes to be certain we can read count # of sets
			 */

			if (sysUptimeMs == null)
				/* 4-7 */
				sysUptimeMs = buffer.readUnsignedInt();

			if (unixSeconds == null)
				/* 8-11 */
				unixSeconds = buffer.readUnsignedInt();

			if (packetSequenceNum == null)
				/* 12-15 */
				packetSequenceNum = buffer.readUnsignedInt();

			if (sourceIdBytes == null) {
				/* 16-19 */
				sourceIdBytes = readBytes(buffer, 4);
				sourceId = Ints.fromByteArray(sourceIdBytes);
			}

			readHeader = true;

		}

		/***** MESSAGES *****/

		while (readIndex < count) {

		}

		return null;
	}

	private byte[] readBytes(ByteBuf buffer, int size) {

		if (currentRawBytesIndex == null) {

			currentRawBytesIndex = 0;
			currentRawBytes = new byte[size];

		}

		while (currentRawBytesIndex < size) {

			buffer.readBytes(currentRawBytes, currentRawBytesIndex, 1);
			currentRawBytesIndex++;

		}

		currentRawBytesIndex = null;
		return currentRawBytes;

	}

	private void resetState() {

		readHeader = false;
		count = null;
		sysUptimeMs = null;
		unixSeconds = null;
		timestamp = 0;
		packetSequenceNum = null;
		sourceIdBytes = null;
		engineType = 0;
		engineId = 0;
		sampling = 0;
		samplingInterval = 0;
		samplingMode = 0;
		packetId = null;
		readerId = null;
		readIndex = 0;
		sourceId = 0;

//		currentFlowsetId = null;
//		currentTemplateFields = null;
//		currentTemplateFieldCount = null;
//		currentTemplateFieldInd = -1;
//		currentTemplateLength = null;
//		currentTemplateBytesToRead = -1;
//		currentTemplateId = null;
//		currentFieldType = null;
//		currentFieldLength = null;
//		currentDataFlowLength = null;
//		currentDataFlowBytesToRead = null;
//		currentDataFlowFieldInd = -1;
//		currentDataFlowFields = null;
//
//		currentOptionsTemplateFields = null;
//		optionsTemplateScopeLength = null;
//		optionsTemplateFieldsLength = null;

		currentRawBytes = null;
		currentRawBytesIndex = null;

		result.clear();
	}

}
