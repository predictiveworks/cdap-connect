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

public enum MessageFormat {

	CAPTURE_LOSS("capture_loss"),
	CONNECTION("connection"),
	DCE_RPC("dce_rpc"),
	DHCP("dhcp"),
	DNP3("dnp3"),
	DNS("dns"),
	DPD("dpd"),
	FILES("files"),
	FTP("ftp"),
	HTTP("http"),
	INTEL("intel"),
	IRC("irc"),
	KERBEROS("kerberos"),
	MODBUS("modbus"),
	MYSQL("mysql"),
	NOTICE("notice"),
	NTLM("ntlm"),
	OCSP("ocsp"),
	PE("pe"),
	RADIUS("radius"),
	RDP("rdp"),
	RFB("rfb"),
	SIP("sip"),
	SMB_CMD("smb_cmd"),
	SMB_FILES("smb_files"),
	SMB_MAPPING("smb_mapping"),
	SMTP("smtp"),
	SNMP("snmp"),
	SOCKS("socks"),
	SSH("ssh"),
	SSL("ssl"),
	STATS("stats"),
	SYSLOG("syslog"),
	TRACEROUTE("traceroute"),
	TUNNEL("tunnel"),
	WEIRD("weird"),
	X509("x509");

	private final String value;

	MessageFormat(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return this.getValue();
	}
	
}
