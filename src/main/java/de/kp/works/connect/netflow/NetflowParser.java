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

import com.google.common.cache.Cache;

import co.cask.cdap.api.data.format.StructuredRecord;
import de.kp.works.connect.netflow.v9.FlowSetTemplate;
import de.kp.works.connect.netflow.v9.FlowSetTemplateCacheKey;
import de.kp.works.connect.netflow.v9.Netflow9Decoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class NetflowParser {

	private NetflowMode mode;
	private final Cache<FlowSetTemplateCacheKey, FlowSetTemplate> flowSetTemplateCache;

	public NetflowParser(NetflowMode mode, int maxTemplateCacheSize, int templateCacheTimeoutMs) {

		this.mode = mode;
		flowSetTemplateCache = Netflow9Decoder.buildTemplateCache(maxTemplateCacheSize, templateCacheTimeoutMs);
	}

	public List<StructuredRecord> parse(byte[] bytes, InetSocketAddress sender, InetSocketAddress recipient)
			throws Exception {

		ByteBuf buffer = Unpooled.wrappedBuffer(bytes);

		int packetLength = buffer.readableBytes();
		if (packetLength < 4) {
			throw new Exception(String.format("Packet must be at least 4 bytes, found: %s", packetLength));
		}

		final List<NetflowMessage> messages = new LinkedList<>();
		final List<StructuredRecord> records = new LinkedList<>();

		final NetflowDecoder decoder = new NetflowDecoder(mode,
				/*
				 * return instance of template cache held by this NetflowParser instance, so
				 * it's shared across multiple invocations of parse this is necessary because
				 * for this parser, we recreate the NetflowCommonDecoder every time, which would
				 * otherwise wipe out the cache across multiple packets
				 */
				() -> flowSetTemplateCache);

		decoder.decode(buffer, messages, sender, recipient);
		for (NetflowMessage message : messages) {
			records.add(message.toRecord());
		}

		return records;

	}

}
