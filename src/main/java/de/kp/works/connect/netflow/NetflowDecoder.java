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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

public class NetflowDecoder {

	protected static final Logger LOG = LoggerFactory.getLogger(NetflowDecoder.class);

	private NetflowMode mode;

	private int version = 0;
	private boolean readVersion = false;

	private Netflow5Decoder netflow5Decoder;
	private Netflow9Decoder netflow9Decoder;

	public NetflowDecoder(NetflowMode mode) {
		this.mode = mode;
	}

	public void decode(ByteBuf buffer, List<NetflowMessage> messages, InetSocketAddress sender,
			InetSocketAddress recipient) throws Exception {

		List<Object> results = new LinkedList<>();
		decode(buffer, results, sender, recipient, true);

		for (Object result : results) {

			if (result == null) {
				LOG.warn("NULL result found from decoding standalone Netflow buffer. Result is skipped.");
				continue;
			}
			if (result instanceof NetflowMessage) {
				messages.add((NetflowMessage) result);

			} else {

			}
			throw new IllegalStateException(
					String.format("Found unexpected object type in results: %s", result.getClass().getName()));
		}

	}

	private void decode(ByteBuf buffer, List<Object> results, InetSocketAddress sender, InetSocketAddress recipient,
			boolean packetLengthCheck) throws Exception {

		int packetLength = buffer.readableBytes();

		if (!readVersion) {
			/* 0-1, for both Netflow 5 and 9 */
			version = buffer.readUnsignedShort();
			readVersion = true;

		}

		switch (version) {
		case 5: {
			if (netflow5Decoder == null)
				netflow5Decoder = new Netflow5Decoder(mode);
			
			results.addAll(netflow5Decoder.parse(version, packetLength, packetLengthCheck, buffer, sender, recipient));
			break;
		}
		case 9: {
			if (netflow9Decoder == null) {
				netflow9Decoder = new Netflow9Decoder(mode);
			}
			results.addAll(netflow9Decoder.parse(version, packetLength, packetLengthCheck, buffer, sender, recipient));
			break;
		}
		default:
			throw new Exception(String.format("The newflow version '%s' is not supported.", version));
		}
	}

}
