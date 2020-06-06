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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.cdap.cdap.api.data.format.StructuredRecord;

public class LogTransform implements Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> {

	private static final long serialVersionUID = 4014403389657429566L;

	protected final ZeekConfig config;	
	@SuppressWarnings("unused")
	private final long batchTime;

	public LogTransform(ZeekConfig config, Long batchTime) {
		this.config = config;
		this.batchTime = batchTime;
	}
	
	@Override
	public StructuredRecord call(ConsumerRecord<byte[], byte[]> input) throws Exception {
		
		String json = new String(input.value(), "UTF-8");
		JsonElement jsonElement = new JsonParser().parse(json);
		
		if (!jsonElement.isJsonObject())
			throw new Exception(String.format("[%s] Zeek log messages must be Json objects.", this.getClass().getName()));
		
		JsonObject jsonObject = jsonElement.getAsJsonObject();
		StructuredRecord record = null;
		
		MessageFormat format = config.getMessageFormat();
		switch (format) {
		case CAPTURE_LOSS:
			record = ZeekUtil.fromCaptureLoss(jsonObject);
			break;
		case CONNECTION:
			record = ZeekUtil.fromConnection(jsonObject);
			break;
		case DCE_RPC:
			record = ZeekUtil.fromDceRpc(jsonObject);
			break;
		case DHCP:
			record = ZeekUtil.fromDhcp(jsonObject);
			break;
		case DNP3:
			record = ZeekUtil.fromDnp3(jsonObject);
			break;
		case DNS:
			record = ZeekUtil.fromDns(jsonObject);
			break;
		/* Dynamic protocol detection failures */
		case DPD:
			record = ZeekUtil.fromDpd(jsonObject);
			break;
		case FTP:
			record = ZeekUtil.fromFtp(jsonObject);
			break;
		case HTTP:
			record = ZeekUtil.fromHttp(jsonObject);
			break;
		case INTEL:
			record = ZeekUtil.fromIntel(jsonObject);
			break;
		case IRC:
			record = ZeekUtil.fromIrc(jsonObject);
			break;
		case KERBEROS:
			record = ZeekUtil.fromKerberos(jsonObject);
			break;
		case MODBUS:
			record = ZeekUtil.fromModbus(jsonObject);
			break;
		case MYSQL:
			record = ZeekUtil.fromMysql(jsonObject);
			break;
		case NOTICE:
			record = ZeekUtil.fromNotice(jsonObject);
			break;
		case NTLM:
			record = ZeekUtil.fromNtlm(jsonObject);
			break;
		case OCSP:
			record = ZeekUtil.fromOcsp(jsonObject);
			break;
		/* Files :: File Analysis Results */
		case FILES:
			record = ZeekUtil.fromFiles(jsonObject);
			break;			
		/* Files :: Portable Executable */
		case PE:
			record = ZeekUtil.fromPe(jsonObject);
			break;
		case RADIUS:
			record = ZeekUtil.fromRadius(jsonObject);
			break;
		case RDP:
			record = ZeekUtil.fromRdp(jsonObject);
			break;
		case RFB:
			record = ZeekUtil.fromRfb(jsonObject);
			break;
		case SIP:
			record = ZeekUtil.fromSip(jsonObject);
			break;
		case SMB_CMD:
			record = ZeekUtil.fromSmbCmd(jsonObject);
			break;
		case SMB_FILES:
			record = ZeekUtil.fromSmbFiles(jsonObject);
			break;
		case SMB_MAPPING:
			record = ZeekUtil.fromSmbMapping(jsonObject);
			break;
		case SMTP:
			record = ZeekUtil.fromSmtp(jsonObject);
			break;
		case SNMP:
			record = ZeekUtil.fromSnmp(jsonObject);
			break;
		case SOCKS:
			record = ZeekUtil.fromSocks(jsonObject);
			break;
		case SSH:
			record = ZeekUtil.fromSsh(jsonObject);
			break;
		case SSL:
			record = ZeekUtil.fromSsl(jsonObject);
			break;
		case STATS:
			record = ZeekUtil.fromStats(jsonObject);
			break;
		case SYSLOG:
			record = ZeekUtil.fromSyslog(jsonObject);
			break;
		case TRACEROUTE:
			record = ZeekUtil.fromTraceroute(jsonObject);
			break;
		case TUNNEL:
			record = ZeekUtil.fromTunnel(jsonObject);
			break;
		case WEIRD:
			record = ZeekUtil.fromWeird(jsonObject);
			break;
		case X509:
			record = ZeekUtil.fromX509(jsonObject);
			break;
		
		}
		
		return record;
	}

}
