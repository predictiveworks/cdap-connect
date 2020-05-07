package de.kp.works.connect.netflow.v9;
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
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.primitives.Ints;

import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.connect.netflow.NetflowMode;
import io.netty.buffer.ByteBuf;

public class Netflow9Decoder {

	/* private static final int V9_HEADER_SIZE = 20; */

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

	private Integer currentFlowsetId = null;

	/* Field templates (completely parsed) */
	private List<Netflow9FieldTemplate> currentTemplateFields = null;

	/* Number of fields in this template */
	private Integer currentTemplateFieldCount = null;

	/* Current index of field being parsed from template */
	private int currentTemplateFieldInd = -1;

	/* Vars for reading specific field in a template flowset */

	/* Length (in bytes) of current template flowset */
	private Integer currentTemplateLength = null;

	/*
	 * Number of bytes left to read for complete parsing of current template flowset
	 */
	private int currentTemplateBytesToRead = -1;

	/* ID of current template flowset */
	private Integer currentTemplateId = null;

	/* Current field type within single field template */
	private Integer currentFieldType = null;

	/* Current field length within single field template */
	private Integer currentFieldLength = null;

	/* Vars for reading a data flowset */

	private Integer currentDataFlowLength = null;
	private Integer currentDataFlowBytesToRead = null;
	private int currentDataFlowFieldInd = -1;

	private List<Netflow9Field> currentDataFlowFields = null;

	/* Vars for reading an options template flowset */
	private List<Netflow9FieldTemplate> currentOptionsTemplateFields = null;

	private Integer optionsTemplateScopeLength = null;
	private Integer optionsTemplateFieldsLength = null;

	private int readIndex = 0;
	private List<Netflow9Message> result = new LinkedList<>();

	private final Cache<FlowSetTemplateCacheKey, FlowSetTemplate> flowSetTemplateCache;

	public Netflow9Decoder(NetflowMode mode, int maxTemplateCacheSize, int templateCacheTimeoutMs) {
		this(mode, () -> buildTemplateCache(maxTemplateCacheSize, templateCacheTimeoutMs));
	}

	public Netflow9Decoder(NetflowMode mode, TemplateCacheProvider templateCacheProvider) {

		this.mode = mode;
		flowSetTemplateCache = templateCacheProvider.getFlowSetTemplateCache();

	}

	public static Cache<FlowSetTemplateCacheKey, FlowSetTemplate> buildTemplateCache(int maxTemplateCacheSize,
			int templateCacheTimeoutMs) {

		CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
		if (maxTemplateCacheSize > 0) {
			cacheBuilder = cacheBuilder.maximumSize(maxTemplateCacheSize);

		}

		if (templateCacheTimeoutMs > 0) {
			cacheBuilder = cacheBuilder.expireAfterAccess(templateCacheTimeoutMs, TimeUnit.MILLISECONDS);

		}

		return cacheBuilder.build();

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

			/*** FLOWSET ***/

			if (currentFlowsetId == null)
				currentFlowsetId = buffer.readUnsignedShort();

			if (currentFlowsetId < 255) {

				/*
				 * This method determines the current template length and updates the templates
				 * bytes to read
				 */
				setCurrentTemplateLength(buffer);

				while (currentTemplateBytesToRead > 0) {

					/*
					 * This method determines the current template id and updates the templates
					 * bytes to read
					 */
					setCurrentTemplateId(buffer);

					/* template or options */
					switch (currentFlowsetId) {
					case 0: {

						/*** flowset template ***/

						if (currentTemplateFieldCount == null) {

							/* Initialization */
							currentTemplateFieldCount = buffer.readUnsignedShort();
							currentTemplateBytesToRead -= 2;

							currentTemplateFields = new LinkedList<>();
							currentTemplateFieldInd = 0;

						}

						while (currentTemplateFieldInd < currentTemplateFieldCount) {

							if (currentFieldType == null) {

								currentFieldType = buffer.readUnsignedShort();
								currentTemplateBytesToRead -= 2;

							}

							if (currentFieldLength == null) {

								currentFieldLength = buffer.readUnsignedShort();
								currentTemplateBytesToRead -= 2;

							}

							final Netflow9FieldTemplate fieldTemplate = new Netflow9FieldTemplate(currentFieldType,
									currentFieldLength);

							currentTemplateFields.add(fieldTemplate);
							currentTemplateFieldInd++;

							currentFieldType = null;
							currentFieldLength = null;

						}

						/*** FLOWSET ***/
						final FlowSetTemplate template = new FlowSetTemplate(currentTemplateId, currentTemplateFields);

						final FlowSetTemplateCacheKey flowsetTemplateCacheKey = new FlowSetTemplateCacheKey(
								FlowKind.FLOWSET, sourceIdBytes, sender, currentTemplateId);

						flowSetTemplateCache.put(flowsetTemplateCacheKey, template);

						readIndex++;

						currentTemplateFieldCount = null;
						currentTemplateFields = null;

						currentTemplateFieldInd = -1;
						currentTemplateId = null;

						break;

					}
					case 1: {

						/*** options template ***/

						if (optionsTemplateScopeLength == null) {

							optionsTemplateScopeLength = buffer.readUnsignedShort();
							currentOptionsTemplateFields = new LinkedList<>();

							currentTemplateBytesToRead -= 2;

						}

						if (optionsTemplateFieldsLength == null) {

							optionsTemplateFieldsLength = buffer.readUnsignedShort();
							currentTemplateBytesToRead -= 2;

						}

						while (currentTemplateBytesToRead > 0) {

							boolean finishedScopeFields = optionsTemplateScopeLength <= 0;

							if (currentFieldType == null) {

								currentFieldType = buffer.readUnsignedShort();
								currentTemplateBytesToRead -= 2;

							}

							if (currentFieldLength == null) {

								currentFieldLength = buffer.readUnsignedShort();
								currentTemplateBytesToRead -= 2;

							}

							Netflow9FieldTemplate optionsFieldTemplate;

							if (finishedScopeFields) {

								optionsFieldTemplate = new Netflow9FieldTemplate(currentFieldType, currentFieldLength);
								optionsTemplateFieldsLength -= 4;

							} else {

								optionsFieldTemplate = Netflow9FieldTemplate.getScopeFieldTemplate(currentFieldType,
										currentFieldLength);
								optionsTemplateScopeLength -= 4;

							}

							currentOptionsTemplateFields.add(optionsFieldTemplate);

							currentFieldType = null;
							currentFieldLength = null;

						}

						/*** OPTIONS ***/
						final FlowSetTemplate optionsTemplate = new FlowSetTemplate(currentTemplateId,
								currentTemplateFields);

						final FlowSetTemplateCacheKey templateCacheKey = new FlowSetTemplateCacheKey(FlowKind.OPTIONS,
								sourceIdBytes, sender, currentTemplateId);

						flowSetTemplateCache.put(templateCacheKey, optionsTemplate);

						currentOptionsTemplateFields = null;
						optionsTemplateScopeLength = null;

						optionsTemplateFieldsLength = null;
						break;

					}
					default:
						throw new Exception(String.format(
								"Unrecognized flowset ID of %s (less than 256, but not 0 [template] or 1 [options])",
								currentFlowsetId));
					}
				}

				/* Done with current template flowset */
				currentTemplateId = null;
				currentTemplateLength = null;

				currentTemplateBytesToRead = -1;

			} else {

				/*** DATA FLOWSET ***/

				/* In this case, the currentFlowsetId is the templateId */
				final int templateId = currentFlowsetId;

				final FlowSetTemplate template = flowSetTemplateCache
						.getIfPresent(new FlowSetTemplateCacheKey(FlowKind.FLOWSET, sourceIdBytes, sender, templateId));

				if (template == null) {
					/*
					 * TODO: handle this case (capture/save the bytes until the template is
					 * received, as per section 9 here: https://www.ietf.org/rfc/rfc3954.txt
					 */
					throw new Exception(String.format(
							"Message field referenced flowset template ID %s, but that was not seen in a template record",
							currentFlowsetId));
				}

				if (currentDataFlowLength == null) {

					currentDataFlowLength = buffer.readUnsignedShort();
					currentDataFlowBytesToRead = currentDataFlowLength - 4;

				}

				while (currentDataFlowBytesToRead > 0) {

					if (currentDataFlowFieldInd < 0 && currentDataFlowBytesToRead < template.getTotalFieldsLength()) {
						/*
						 * We aren't in the middle of parsing a field (currentDataFlowFieldInd < 0) AND
						 * there isn't enough data left for a complete record (2nd clause) so this must
						 * be padding; just skip it
						 */
						readBytes(buffer, currentDataFlowBytesToRead);
						break;
					}

					if (currentDataFlowFields == null) {

						/* Initialization */
						currentDataFlowFields = new LinkedList<>();
						currentDataFlowFieldInd = 0;

					}

					final List<Netflow9FieldTemplate> fieldTemplates = template.getFieldTemplates();
					final int numDataFlowFields = fieldTemplates.size();

					while (currentDataFlowFieldInd < numDataFlowFields) {

						Netflow9FieldTemplate fieldTemplate = fieldTemplates.get(currentDataFlowFieldInd);
						Netflow9Field field = decodeField(buffer, fieldTemplate, mode);

						currentDataFlowFields.add(field);
						currentDataFlowBytesToRead -= fieldTemplate.getLength();

						/* done reading a single field */
						currentDataFlowFieldInd++;

					}

					/* Done reading a flow record */

					Netflow9Message message = new Netflow9Message();
					message.setNetflowMode(mode);

					message.setFlowKind(FlowKind.FLOWSET);
					message.setVersion(9);

					message.setSender(sender);
					message.setRecipient(recipient);

					/*** header fields ***/

					message.setUptime(sysUptimeMs);
					message.setUnixSeconds(unixSeconds);

					message.setPacketSeq(packetSequenceNum);

					message.setSourceId(sourceId);
					message.setSourceIdRaw(sourceIdBytes);

					/*** data fields ***/
					message.setFlowTemplateId(templateId);
					message.setFlowFields(currentDataFlowFields);

					result.add(message);
					readIndex++;

					currentDataFlowFields = null;
					currentDataFlowFieldInd = -1;

				}

				/* Done reading all flow records */
				currentDataFlowLength = null;

			}

			/* Done with this flowset, which ever type it was */
			currentFlowsetId = null;
		}

		/*
		 * if we reached this point without any further Signal errors, we have finished
		 * consuming
		 */
		LinkedList<Netflow9Message> output = new LinkedList<>(result);
		resetState();

		return output;

	}

	private Netflow9Field decodeField(ByteBuf buffer, Netflow9FieldTemplate fieldTemplate, NetflowMode mode)
			throws Exception {

		Netflow9FieldType fieldType = fieldTemplate.getType();
		int length = fieldTemplate.getLength();

		byte[] rawBytes = readBytes(buffer, length);
		Netflow9Field interpretedValueField = new Netflow9Field(fieldTemplate);

		if (mode != NetflowMode.RAW) {

			if (fieldType == null) {

				/*
				 * Just use raw bytes if unable to recognize a known type; this also results in
				 * a generic field name, derived from the template id
				 */
				interpretedValueField.setBytes(rawBytes);

			} else {
				switch (fieldType) {
				case IN_BYTES:
				case IN_PKTS:
				case FLOWS:
					interpretedValueField.setBigInteger(rawBytes, Schema.Type.LONG);
					break;
				case PROTOCOL:
				case SRC_TOS:
				case TCP_FLAGS:
					interpretedValueField.setByte(rawBytes);
					break;
				case L4_SRC_PORT:
				case L4_DST_PORT:
					interpretedValueField.setShort(rawBytes);
					break;
				case SRC_MASK:
				case DST_MASK:
					interpretedValueField.setByte(rawBytes);
					break;
				case INPUT_SNMP:
				case OUTPUT_SNMP:
					interpretedValueField.setBigInteger(rawBytes, Schema.Type.INT);
					break;
				case IPV4_SRC_ADDR:
				case IPV4_DST_ADDR:
				case IPV4_NEXT_HOP:
				case BGP_IPV4_NEXT_HOP:
					interpretedValueField.setIpV4Address(rawBytes);
					break;
				case SRC_AS:
				case DST_AS:
					interpretedValueField.setBigInteger(rawBytes, Schema.Type.INT);
					break;
				case MUL_DST_PKTS:
				case MUL_DST_BYTES:
					interpretedValueField.setBigInteger(rawBytes, Schema.Type.LONG);
					break;
				case LAST_SWITCHED:
				case FIRST_SWITCHED:
					interpretedValueField.setLong(rawBytes);
					break;
				case OUT_BYTES:
				case OUT_PKTS:
					interpretedValueField.setBigInteger(rawBytes, Schema.Type.LONG);
					break;
				case MIN_PKT_LNGTH:
				case MAX_PKT_LNGTH:
					interpretedValueField.setShort(rawBytes);
					break;
				case IPV6_SRC_ADDR:
				case IPV6_DST_ADDR:
				case IPV6_NEXT_HOP:
				case BGP_IPV6_NEXT_HOP:
					interpretedValueField.setIPV6Address(rawBytes);
					break;
				case IPV6_SRC_MASK:
				case IPV6_DST_MASK:
					interpretedValueField.setByte(rawBytes);
					break;
				case IPV6_FLOW_LABEL:
					interpretedValueField.setBytes(rawBytes);
					break;
				case ICMP_TYPE:
					interpretedValueField.setShort(rawBytes);
					break;
				case MUL_IGMP_TYPE:
					interpretedValueField.setByte(rawBytes);
					break;
				case SAMPLING_INTERVAL:
					interpretedValueField.setLong(rawBytes);
					break;
				case SAMPLING_ALGORITHM:
					interpretedValueField.setByte(rawBytes);
					break;
				case FLOW_ACTIVE_TIMEOUT:
				case FLOW_INACTIVE_TIMEOUT:
					interpretedValueField.setShort(rawBytes);
					break;
				case ENGINE_TYPE:
				case ENGINE_ID:
					interpretedValueField.setByte(rawBytes);
					break;
				case TOTAL_BYTES_EXP:
				case TOTAL_PKTS_EXP:
				case TOTAL_FLOWS_EXP:
					interpretedValueField.setBigInteger(rawBytes, Schema.Type.LONG);
					break;
				case IPV4_SRC_PREFIX:
				case IPV4_DST_PREFIX:
					interpretedValueField.setLong(rawBytes);
					break;
				case MPLS_TOP_LABEL_TYPE:
					interpretedValueField.setByte(rawBytes);
					break;
				case MPLS_TOP_LABEL_IP_ADDR:
					interpretedValueField.setLong(rawBytes);
					break;
				case FLOW_SAMPLER_ID:
				case FLOW_SAMPLER_MODE:
					interpretedValueField.setByte(rawBytes);
					break;
				case FLOW_SAMPLER_RANDOM_INTERVAL:
					interpretedValueField.setLong(rawBytes);
					break;
				case MIN_TTL:
				case MAX_TTL:
					interpretedValueField.setByte(rawBytes);
					break;
				case IPV4_IDENT:
					interpretedValueField.setShort(rawBytes);
					break;
				case DST_TOS:
					interpretedValueField.setByte(rawBytes);
					break;
				case IN_SRC_MAC:
				case OUT_DST_MAC:
				case IN_DST_MAC:
				case OUT_SRC_MAC:
					interpretedValueField.setMacAddress(rawBytes);
					break;
				case SRC_VLAN:
				case DST_VLAN:
					interpretedValueField.setShort(rawBytes);
					break;
				case IP_PROTOCOL_VERSION:
				case DIRECTION:
					interpretedValueField.setByte(rawBytes);
					break;
				case IPV6_OPTION_HEADERS:
					interpretedValueField.setLong(rawBytes);
					break;
				case MPLS_LABEL_1:
				case MPLS_LABEL_2:
				case MPLS_LABEL_3:
				case MPLS_LABEL_4:
				case MPLS_LABEL_5:
				case MPLS_LABEL_6:
				case MPLS_LABEL_7:
				case MPLS_LABEL_8:
				case MPLS_LABEL_9:
				case MPLS_LABEL_10:
					interpretedValueField.setBytes(rawBytes);
					break;
				case IF_NAME:
				case IF_DESC:
				case SAMPLER_NAME:
					interpretedValueField.setString(rawBytes);
					break;
				case IN_PERMANENT_BYTES:
				case IN_PERMANENT_PKTS:
					interpretedValueField.setBigInteger(rawBytes, Schema.Type.LONG);
					break;
				case FRAGMENT_OFFSET:
					interpretedValueField.setShort(rawBytes);
					break;
				case FORWARDING_STATUS:
					interpretedValueField.setByte(rawBytes);
					break;
				case MPLS_PAL_RD:
					interpretedValueField.setBytes(rawBytes);
					break;
				case MPLS_PREFIX_LEN:
					interpretedValueField.setByte(rawBytes);
					break;
				case SRC_TRAFFIC_INDEX:
				case DST_TRAFFIC_INDEX:
					interpretedValueField.setLong(rawBytes);
					break;
				case APPLICATION_DESCRIPTION:
					interpretedValueField.setString(rawBytes);
					break;
				case APPLICATION_TAG:
					interpretedValueField.setBytes(rawBytes);
					break;
				case APPLICATION_NAME:
					interpretedValueField.setString(rawBytes);
					break;
				case POSTIP_DIFF_SERV_CODE_POINTS:
					interpretedValueField.setByte(rawBytes);
					break;
				case REPLICATION_FACTOR:
					interpretedValueField.setLong(rawBytes);
					break;
				case LAYER2_PACKET_SECTION_OFFSET:
				case LAYER2_PACKET_SECTION_SIZE:
				case LAYER2_PACKET_SECTION_DATA:
					interpretedValueField.setBytes(rawBytes);
					break;
				default:
					interpretedValueField.setBytes(rawBytes);
					break;
				}
			}
		}

		return interpretedValueField;

	}

	private void setCurrentTemplateId(ByteBuf buffer) {

		if (currentTemplateId == null) {

			currentTemplateId = buffer.readUnsignedShort();
			currentTemplateBytesToRead -= 2;

		}

	}

	private void setCurrentTemplateLength(ByteBuf buffer) {

		if (currentTemplateLength == null) {

			currentTemplateLength = buffer.readUnsignedShort();
			currentTemplateBytesToRead = currentTemplateLength - 4;

		}

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
		packetSequenceNum = null;
		sourceIdBytes = null;
		readIndex = 0;
		sourceId = 0;

		currentFlowsetId = null;
		currentTemplateFields = null;
		currentTemplateFieldCount = null;
		currentTemplateFieldInd = -1;
		currentTemplateLength = null;
		currentTemplateBytesToRead = -1;
		currentTemplateId = null;
		currentFieldType = null;
		currentFieldLength = null;
		currentDataFlowLength = null;
		currentDataFlowBytesToRead = null;
		currentDataFlowFieldInd = -1;
		currentDataFlowFields = null;

		currentOptionsTemplateFields = null;
		optionsTemplateScopeLength = null;
		optionsTemplateFieldsLength = null;

		currentRawBytes = null;
		currentRawBytesIndex = null;

		result.clear();
	}

}
